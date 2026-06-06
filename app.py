"""
Mailgun Inbound Webhook – empfängt Serviceberichte per E-Mail und speichert sie in der DB.

Ablauf:
  Mail mit PDF-Anhang → Mailgun → POST /webhook/mailgun → Claude analysiert → PostgreSQL

Setup:
  1. Railway: Neues Projekt, diesen webhook/-Ordner deployen
  2. Env-Variablen in Railway setzen (siehe .env.example)
  3. Mailgun: Route anlegen → Forward to → https://<deine-railway-url>/webhook/mailgun
"""

import os
import hmac
import hashlib
import tempfile
import logging
import json
import base64

from flask import Flask, request, jsonify

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

import anthropic
import psycopg2

# ── Konfiguration ─────────────────────────────────────────────────────────────
ANTHROPIC_API_KEY   = os.environ["ANTHROPIC_API_KEY"]
DATABASE_URL        = os.environ["DATABASE_URL"]
MAILGUN_SIGNING_KEY = os.getenv("MAILGUN_SIGNING_KEY", "")

app    = Flask(__name__)
client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════════════════════
#  Sicherheit
# ═══════════════════════════════════════════════════════════════════════════════

def _verify_mailgun(token: str, timestamp: str, signature: str) -> bool:
    """Prüft die Mailgun HMAC-Signatur. Gibt True zurück wenn MAILGUN_SIGNING_KEY leer."""
    if not MAILGUN_SIGNING_KEY:
        log.warning("MAILGUN_SIGNING_KEY nicht gesetzt – Signaturprüfung übersprungen")
        return True
    data     = f"{timestamp}{token}".encode()
    computed = hmac.new(MAILGUN_SIGNING_KEY.encode(), data, hashlib.sha256).hexdigest()
    return hmac.compare_digest(computed, signature)

# ═══════════════════════════════════════════════════════════════════════════════
#  PDF-Analyse via Claude
# ═══════════════════════════════════════════════════════════════════════════════

_PROMPT = """Du analysierst einen Koenig & Bauer Servicebericht.
Extrahiere alle verfuegbaren Informationen und gib sie als JSON zurueck.
Antworte NUR mit dem JSON-Objekt, ohne Erklaerungen oder Markdown.
WICHTIG: Verwende in Texten keine Anfuehrungszeichen. Ersetze " durch ' in allen Textwerten.

Struktur:
{
  "techniker": "Name des Technikers aus Work Report is prepared by",
  "kunde": {
    "name": "Firmenname Account",
    "adresse": "Vollstaendige Adresse",
    "kontakt": "Ansprechpartner Kontakt"
  },
  "terminnummer": "SA-XXXXX",
  "betreff": "Betreff-Feld",
  "datum_start": "TT.MM.JJJJ",
  "datum_ende": "TT.MM.JJJJ",
  "loesung_zusatztext": "Inhalt des Loesungs-Feldes falls vorhanden",
  "abschlusstext": "Abschlusstext falls vorhanden",
  "geraete": [
    {
      "position": "00738267-01",
      "seriennummer": "MID010-039372",
      "geraetetyp": "ALPHAJET EVO 55u DYE V3",
      "status": "Erledigt oder Nicht Erledigt",
      "arbeitszeit_stunden": 2.5,
      "arbeitstext": "Beschreibung der durchgefuehrten Arbeiten"
    }
  ],
  "ersatzteile": [
    {
      "position": "00000017",
      "seriennummer": "MID010-038028",
      "produktcode": "1039.4238",
      "produktname": "SERVICE SET FILTERS V3",
      "menge": 1
    }
  ]
}

Regeln:
- arbeitszeit_stunden: Zahl aus Tatsaechliche Dauer, 0 wenn leer
- Alle Geraete aus der Belegposten-Liste erfassen
- Alle Ersatzteile aus Spare Parts Produktverbrauch erfassen
- Falls ein Feld nicht vorhanden ist null verwenden
- Keine Anfuehrungszeichen innerhalb von Textwerten"""


def analysiere_pdf(pdf_bytes: bytes) -> dict:
    pdf_b64 = base64.standard_b64encode(pdf_bytes).decode()
    response = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=16000,
        messages=[{
            "role": "user",
            "content": [
                {
                    "type": "document",
                    "source": {
                        "type": "base64",
                        "media_type": "application/pdf",
                        "data": pdf_b64,
                    },
                },
                {"type": "text", "text": _PROMPT},
            ],
        }],
    )
    text = response.content[0].text.strip()
    text = text.replace("```json", "").replace("```", "").strip()
    return json.loads(text)


def _datum(s):
    if not s:
        return None
    try:
        t = s.split(".")
        return f"{t[2]}-{t[1]}-{t[0]}"
    except Exception:
        return s


def speichere_in_db(daten: dict) -> int:
    conn = psycopg2.connect(DATABASE_URL)
    cur  = conn.cursor()

    cur.execute(
        "INSERT INTO techniker (name) VALUES (%s) "
        "ON CONFLICT (name) DO UPDATE SET name=EXCLUDED.name RETURNING id",
        (daten["techniker"],)
    )
    techniker_id = cur.fetchone()[0]

    cur.execute(
        """INSERT INTO kunden (name, kundennummer, adresse)
           VALUES (%s, %s, %s)
           ON CONFLICT (kundennummer) DO UPDATE SET name=EXCLUDED.name RETURNING id""",
        (daten["kunde"]["name"], daten["terminnummer"], daten["kunde"]["adresse"])
    )
    kunde_id = cur.fetchone()[0]

    cur.execute(
        """INSERT INTO serviceberichte
               (techniker_id, kunde_id, datum, geraet_nr, arbeitszeit, zusatztext)
           VALUES (%s, %s, %s, %s, %s, %s) RETURNING id""",
        (
            techniker_id,
            kunde_id,
            _datum(daten.get("datum_ende")),
            daten["terminnummer"],
            sum(g.get("arbeitszeit_stunden") or 0 for g in daten.get("geraete", [])),
            daten.get("loesung_zusatztext") or daten.get("abschlusstext"),
        )
    )
    bericht_id = cur.fetchone()[0]

    for et in daten.get("ersatzteile", []):
        cur.execute(
            """INSERT INTO ersatzteile
                   (techniker_id, bezeichnung, teilenummer, menge, datum)
               VALUES (%s, %s, %s, %s, %s)""",
            (
                techniker_id,
                et.get("produktname"),
                et.get("produktcode"),
                et.get("menge"),
                _datum(daten.get("datum_ende")),
            )
        )

    conn.commit()
    cur.close()
    conn.close()
    return bericht_id

# ═══════════════════════════════════════════════════════════════════════════════
#  Webhook-Endpoint
# ═══════════════════════════════════════════════════════════════════════════════

@app.route("/webhook/mailgun", methods=["POST"])
def mailgun_inbound():
    # ── Signatur prüfen ───────────────────────────────────────────────────────
    token     = request.form.get("token",     "")
    timestamp = request.form.get("timestamp", "")
    signature = request.form.get("signature", "")

    if not _verify_mailgun(token, timestamp, signature):
        log.warning("Ungültige Mailgun-Signatur – Anfrage abgewiesen")
        return jsonify({"error": "forbidden"}), 403

    sender  = request.form.get("from",    "?")
    subject = request.form.get("subject", "")
    n_att   = int(request.form.get("attachment-count", 0))
    log.info(f"Mail von {sender!r}  Betreff: {subject!r}  Anhänge: {n_att}")

    if n_att == 0:
        return jsonify({"status": "no_attachments"}), 200

    processed, errors = [], []

    for i in range(1, n_att + 1):
        f = request.files.get(f"attachment-{i}")
        if not f:
            continue
        if not f.filename.lower().endswith(".pdf"):
            log.info(f"  Anhang {i} ({f.filename}) ist kein PDF – übersprungen")
            continue

        log.info(f"  Analysiere {f.filename} …")
        pdf_bytes = f.read()

        try:
            daten      = analysiere_pdf(pdf_bytes)
            bericht_id = speichere_in_db(daten)
            terminnr   = daten.get("terminnummer", "?")
            kunde      = (daten.get("kunde") or {}).get("name", "?")
            log.info(f"  ✅ Bericht-ID {bericht_id}: {terminnr} – {kunde}")
            processed.append({"terminnr": terminnr, "kunde": kunde, "id": bericht_id})
        except Exception as e:
            log.error(f"  ❌ Fehler bei {f.filename}: {e}")
            errors.append({"file": f.filename, "error": str(e)})

    status = "ok" if not errors else ("partial" if processed else "error")
    return jsonify({"status": status, "processed": processed, "errors": errors}), 200


# ── Health-Check für Railway ──────────────────────────────────────────────────
@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"}), 200


if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    log.info(f"Server startet auf Port {port}")
    app.run(host="0.0.0.0", port=port, debug=False)
