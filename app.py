"""
Inbound Webhook – empfängt Serviceberichte per E-Mail und speichert sie in der DB.
Unterstützt: Mailgun + Brevo

Ablauf:
  Mail mit PDF-Anhang → Mailgun/Brevo → POST /webhook/mailgun oder /webhook/brevo → Claude → PostgreSQL

Setup:
  1. Railway: Env-Variablen setzen (ANTHROPIC_API_KEY, DATABASE_URL, MAILGUN_SIGNING_KEY)
  2. Mailgun: Route → Forward → https://swebhook.up.railway.app/webhook/mailgun
"""

import os
import hmac
import hashlib
import logging
import json
import base64
import threading

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
WEBHOOK_TOKEN       = os.getenv("WEBHOOK_TOKEN", "")

app    = Flask(__name__)
client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

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
#  Hilfsfunktionen
# ═══════════════════════════════════════════════════════════════════════════════

def _verify_mailgun(token: str, timestamp: str, signature: str) -> bool:
    if not MAILGUN_SIGNING_KEY:
        log.warning("MAILGUN_SIGNING_KEY nicht gesetzt – Signaturprüfung übersprungen")
        return True
    data     = f"{timestamp}{token}".encode()
    computed = hmac.new(MAILGUN_SIGNING_KEY.encode(), data, hashlib.sha256).hexdigest()
    return hmac.compare_digest(computed, signature)


def _process_pdf(pdf_bytes: bytes, filename: str) -> dict:
    """PDF analysieren und in DB speichern – gemeinsam für alle Endpoints."""
    daten      = analysiere_pdf(pdf_bytes)
    bericht_id = speichere_in_db(daten)
    terminnr   = daten.get("terminnummer", "?")
    kunde      = (daten.get("kunde") or {}).get("name", "?")
    log.info(f"  ✅ Bericht-ID {bericht_id}: {terminnr} – {kunde}")
    return {"terminnr": terminnr, "kunde": kunde, "id": bericht_id}


# ═══════════════════════════════════════════════════════════════════════════════
#  Webhook-Endpoint (Mailgun)
# ═══════════════════════════════════════════════════════════════════════════════

@app.route("/webhook/mailgun", methods=["POST"])
def mailgun_inbound():
    token     = request.form.get("token",     "")
    timestamp = request.form.get("timestamp", "")
    signature = request.form.get("signature", "")

    if not _verify_mailgun(token, timestamp, signature):
        log.warning("Ungültige Mailgun-Signatur – abgewiesen")
        return jsonify({"error": "forbidden"}), 403

    sender  = request.form.get("from",    "?")
    subject = request.form.get("subject", "")
    n_att   = int(request.form.get("attachment-count", 0))
    log.info(f"Mailgun: von {sender!r}  Betreff: {subject!r}  Anhänge: {n_att}")

    if n_att == 0:
        return jsonify({"status": "no_attachments"}), 200

    # PDFs sofort einlesen (request-Kontext lebt nur während des Requests)
    pdfs = []
    for i in range(1, n_att + 1):
        f = request.files.get(f"attachment-{i}")
        if f and f.filename.lower().endswith(".pdf"):
            pdfs.append((f.filename, f.read()))

    # Sofort 200 zurückgeben – Analyse läuft im Hintergrund
    def process_in_background():
        for filename, pdf_bytes in pdfs:
            log.info(f"  Analysiere {filename} …")
            try:
                _process_pdf(pdf_bytes, filename)
            except Exception as e:
                log.error(f"  ❌ {filename}: {e}")

    threading.Thread(target=process_in_background, daemon=True).start()
    return jsonify({"status": "accepted", "pdfs": len(pdfs)}), 200


# ═══════════════════════════════════════════════════════════════════════════════
#  Webhook-Endpoint (Brevo)
# ═══════════════════════════════════════════════════════════════════════════════

@app.route("/webhook/brevo", methods=["POST"])
def brevo_inbound():
    # ── Token-Prüfung ─────────────────────────────────────────────────────────
    if WEBHOOK_TOKEN and request.args.get("token") != WEBHOOK_TOKEN:
        log.warning("Ungültiger Token – Anfrage abgewiesen")
        return jsonify({"error": "forbidden"}), 403

    data = request.get_json(force=True, silent=True) or {}

    sender  = (data.get("From") or {}).get("Address", "?")
    subject = data.get("Subject", "")
    attachments = data.get("Attachments") or []

    log.info(f"Mail von {sender!r}  Betreff: {subject!r}  Anhänge: {len(attachments)}")

    if not attachments:
        return jsonify({"status": "no_attachments"}), 200

    processed, errors = [], []

    for att in attachments:
        name         = att.get("Name", "")
        content_type = att.get("ContentType", "")
        b64_content  = att.get("Base64Content", "")

        if not name.lower().endswith(".pdf") and "pdf" not in content_type.lower():
            log.info(f"  {name} ist kein PDF – übersprungen")
            continue

        log.info(f"  Analysiere {name} …")
        try:
            processed.append(_process_pdf(base64.b64decode(b64_content), name))
        except Exception as e:
            log.error(f"  ❌ {name}: {e}")
            errors.append({"file": name, "error": str(e)})

    status = "ok" if not errors else ("partial" if processed else "error")
    return jsonify({"status": status, "processed": processed, "errors": errors}), 200


# ── Health-Check ──────────────────────────────────────────────────────────────
@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"}), 200


if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    log.info(f"Server startet auf Port {port}")
    app.run(host="0.0.0.0", port=port, debug=False)
