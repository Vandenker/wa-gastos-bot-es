# main.py
import os
import re
import hmac
import json
import hashlib
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, Optional

import requests
import pytz
import phonenumbers
from phonenumbers import PhoneNumberFormat
from fastapi import FastAPI, Request, HTTPException, Query
from fastapi.responses import PlainTextResponse, JSONResponse
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text

# -------------------- Configuración --------------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("wa-gastos-bot")

WHATSAPP_TOKEN = os.getenv("WHATSAPP_TOKEN", "").strip()
PHONE_NUMBER_ID = os.getenv("PHONE_NUMBER_ID", "").strip()
APP_SECRET = os.getenv("APP_SECRET", "").strip()  # si lo dejas vacío, no valida firma
VERIFY_TOKEN = os.getenv("VERIFY_TOKEN", "miverify123").strip()
GRAPH_VERSION = os.getenv("GRAPH_API_VERSION", "v20.0").strip()

# BD Neon (SQLAlchemy async + psycopg)
# Formato recomendado: postgresql+psycopg://USER:PASS@HOST/DB?sslmode=require
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
if not DATABASE_URL:
    log.warning("DATABASE_URL vacío; las operaciones de BD fallarán.")
engine = create_async_engine(DATABASE_URL, pool_pre_ping=True, future=True) if DATABASE_URL else None

# Zona horaria local (Argentina)
TZ_NAME = os.getenv("TZ", "America/Argentina/Buenos_Aires")
ARG_TZ = pytz.timezone(TZ_NAME)

# Estados en memoria (free tier)
SESSIONS: Dict[str, Dict[str, Any]] = {}
SESSION_TTL_MIN = int(os.getenv("SESSION_TTL_MIN", "2"))

MEDIOS_VALIDOS = ["efectivo", "debito", "credito", "transferencia"]
MONEDA_DEFAULT = "ARS"

app = FastAPI(title="WA Gastos Bot (ES)")

# -------------------- Utilidades --------------------
def now_local() -> datetime:
    return datetime.now(ARG_TZ)

def normalize_text(s: str) -> str:
    return (s or "").strip().lower()

def normalize_phone_ar(raw: str, default_region: str = "AR") -> str:
    """
    Normaliza números AR en muchos formatos a E.164 SIN '+' (solo dígitos).
    Soporta: '+54 9 11 ...', '+54 11 15 ...', '54 9 11 ...', '011 15 ...', '54911...'
    """
    if not raw:
        raise ValueError("Número vacío")

    # Si ya llega solo dígitos y empieza con 549 (típico wa_id), úsalo
    just_digits = re.sub(r"\D", "", raw)
    if just_digits.startswith("549") and 11 <= len(just_digits) <= 13:
        return just_digits

    try:
        # Dejar que phonenumbers haga la magia
        pn = phonenumbers.parse(raw, None if raw.strip().startswith("+") else default_region)
        if phonenumbers.is_valid_number(pn):
            e164 = phonenumbers.format_number(pn, PhoneNumberFormat.E164)  # +54911...
            return re.sub(r"\D", "", e164)
    except Exception:
        pass

    # Fallback manual
    d = just_digits
    if d.startswith("54"):
        rest = d[2:]
        if rest.startswith("0"):
            rest = rest[1:]
        # quitar un '15' temprano
        if rest.startswith("11") and rest[2:4] == "15":
            rest = rest[:2] + rest[4:]
        elif "15" in rest[:6]:
            rest = rest.replace("15", "", 1)
        if not rest.startswith("9"):
            rest = "9" + rest
        return "54" + rest
    # sin 54, asumimos AR
    if d.startswith("0"):
        d = d[1:]
    d = d.replace("15", "", 1)
    return "549" + d

def verify_signature(app_secret: str, request_body: bytes, header_signature: str) -> bool:
    if not app_secret:
        return True  # sin secreto, no validamos
    if not header_signature or not header_signature.startswith("sha256="):
        return False
    try:
        sent_sig = header_signature.split("=", 1)[1]
        mac = hmac.new(app_secret.encode(), msg=request_body, digestmod=hashlib.sha256)
        expected = mac.hexdigest()
        return hmac.compare_digest(sent_sig, expected)
    except Exception:
        return False

def wa_send_text(to_raw: str, body: str) -> None:
    if not (WHATSAPP_TOKEN and PHONE_NUMBER_ID):
        log.error("Faltan WHATSAPP_TOKEN o PHONE_NUMBER_ID")
        return
    to = normalize_phone_ar(to_raw)
    url = f"https://graph.facebook.com/{GRAPH_VERSION}/{PHONE_NUMBER_ID}/messages"
    headers = {"Authorization": f"Bearer {WHATSAPP_TOKEN}", "Content-Type": "application/json"}
    payload = {
        "messaging_product": "whatsapp",
        "to": to,
        "type": "text",
        "text": {"body": body[:4000], "preview_url": False},
    }
    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=25)
        if resp.status_code >= 400:
            log.error("❌ Error enviando mensaje: %s %s", resp.status_code, resp.text)
    except Exception as e:
        log.exception("Error al llamar a Graph API: %s", e)

def session(of_user: str) -> Dict[str, Any]:
    s = SESSIONS.get(of_user)
    now = now_local()
    if not s or s.get("expires_at") < now:
        s = {"state": "menu", "data": {}, "expires_at": now + timedelta(minutes=SESSION_TTL_MIN)}
        SESSIONS[of_user] = s
    else:
        s["expires_at"] = now + timedelta(minutes=SESSION_TTL_MIN)
    return s

def parse_monto_moneda(s: str) -> (Optional[float], str):
    raw = (s or "").strip().replace(".", "").replace(",", ".")
    parts = raw.split()
    if not parts:
        return None, MONEDA_DEFAULT
    try:
        monto = float(parts[0])
    except Exception:
        return None, MONEDA_DEFAULT
    moneda = parts[1].upper() if len(parts) > 1 else MONEDA_DEFAULT
    return monto, moneda

def parse_fecha_hora_local(s: str) -> Optional[datetime]:
    fmts = ["%Y-%m-%d %H:%M", "%d/%m/%Y %H:%M", "%Y-%m-%d", "%d/%m/%Y"]
    s = (s or "").strip()
    for fmt in fmts:
        try:
            dt_naive = datetime.strptime(s, fmt)
            return ARG_TZ.localize(dt_naive)
        except Exception:
            continue
    return None

# -------------------- BD Helpers --------------------
async def mark_processed(msg_id: str) -> bool:
    """True si la insert fue nueva (o si no hay tabla), False si ya existía."""
    if not engine:
        return True
    try:
        q = text("""                INSERT INTO mensajes_procesados (clave_mensaje)
            VALUES (:m)
            ON CONFLICT (clave_mensaje) DO NOTHING
        """)
        async with engine.begin() as conn:
            res = await conn.execute(q, {"m": msg_id})
        return True
    except Exception as e:
        log.warning("No se pudo registrar mensaje procesado (quizá falta la tabla): %s", e)
        return True

async def insert_gasto(data: Dict[str, Any], texto_original: str, origen: str) -> int:
    if not engine:
        raise RuntimeError("Sin DATABASE_URL configurada")
    q = text("""            INSERT INTO gastos
        (monto, moneda, descripcion, categoria, comercio,
         medio_pago, banco, marca_tarjeta, cuenta_pago,
         texto_original, whatsapp_origen)
        VALUES
        (:monto, :moneda, :descripcion, :categoria, :comercio,
         :medio_pago, :banco, :marca_tarjeta, :cuenta_pago,
         :texto_original, :whatsapp_origen)
        RETURNING clave
    """)
    params = {
        "monto": data.get("monto"),
        "moneda": data.get("moneda", MONEDA_DEFAULT),
        "descripcion": data.get("descripcion"),
        "categoria": data.get("categoria"),
        "comercio": data.get("comercio"),
        "medio_pago": data.get("medio_pago"),
        "banco": data.get("banco"),
        "marca_tarjeta": data.get("marca_tarjeta"),
        "cuenta_pago": data.get("cuenta_pago"),
        "texto_original": texto_original,
        "whatsapp_origen": origen,
    }
    async with engine.begin() as conn:
        res = await conn.execute(q, params)
        row = res.fetchone()
        return int(row[0]) if row else 0

async def resumen_historial(fd: Optional[datetime], fh: Optional[datetime], medio: Optional[str]) -> str:
    if not engine:
        return "BD no configurada."
    conds = []
    params = {}
    if fd:
        conds.append("fecha_hora_utc >= :fd")
        params["fd"] = fd.astimezone(pytz.UTC)
    if fh:
        conds.append("fecha_hora_utc <= :fh")
        params["fh"] = fh.astimezone(pytz.UTC)
    if medio:
        conds.append("medio_pago = :mp")
        params["mp"] = medio
    where = ("WHERE " + " AND ".join(conds)) if conds else ""
    q = text(f"""            SELECT COALESCE(SUM(monto),0) AS total, COUNT(*) AS n
        FROM gastos
        {where}
    """ )
    try:
        async with engine.begin() as conn:
            res = await conn.execute(q, params)
            row = res.fetchone()
        total = float(row[0]) if row and row[0] is not None else 0.0
        n = int(row[1]) if row and row[1] is not None else 0
        return f"Movimientos: {n}\nTotal: {total:.2f}"
    except Exception as e:
        log.error("Error consultando historial: %s", e)
        return "No pude consultar ahora. Intentá más tarde."

# -------------------- Endpoints --------------------
@app.get("/", response_class=PlainTextResponse)
def root():
    return "OK - WA Gastos Bot"

@app.get("/webhook", response_class=PlainTextResponse)
def verify_webhook(
    hub_mode: str = Query(None, alias="hub.mode"),
    hub_challenge: str = Query(None, alias="hub.challenge"),
    hub_verify_token: str = Query(None, alias="hub.verify_token"),
):
    if hub_mode == "subscribe" and hub_verify_token == VERIFY_TOKEN:
        log.info("Webhook verificado correctamente.")
        return hub_challenge or ""
    raise HTTPException(status_code=403, detail="Token de verificación inválido.")

@app.post("/webhook")
async def webhook(request: Request):
    raw = await request.body()
    if APP_SECRET:
        sig = request.headers.get("x-hub-signature-256", "")
        if not verify_signature(APP_SECRET, raw, sig):
            log.warning("Firma de webhook inválida.")
            raise HTTPException(status_code=401, detail="Invalid signature")

    try:
        data = json.loads(raw.decode("utf-8") or "{}")
    except Exception:
        data = {}

    log.info("Evento entrante: %s", json.dumps(data, ensure_ascii=False))

    try:
        for entry in data.get("entry", []):
            for change in entry.get("changes", []):
                value = change.get("value", {})
                messages = value.get("messages", [])
                if not messages:
                    continue
                msg = messages[0]
                msg_id = msg.get("id")
                from_wa = msg.get("from") or (value.get("contacts", [{}])[0].get("wa_id") if value.get("contacts") else None)
                msg_type = msg.get("type")

                if not from_wa or not msg_id:
                    continue

                # De-duplicación
                await mark_processed(msg_id)

                # Texto del usuario
                user_text = ""
                if msg_type == "text":
                    user_text = (msg.get("text") or {}).get("body", "") or ""
                elif msg_type == "interactive":
                    inter = msg.get("interactive", {})
                    if inter.get("type") == "button_reply":
                        user_text = inter["button_reply"].get("title", "")
                    elif inter.get("type") == "list_reply":
                        user_text = inter["list_reply"].get("title", "")

                # Sesión y flujo
                sess = session(from_wa)
                state = sess["state"]
                data_acc = sess["data"]

                def goto(st: str):
                    sess["state"] = st
                    sess["expires_at"] = now_local() + timedelta(minutes=SESSION_TTL_MIN)

                # Entrada al menú
                if state == "menu":
                    wa_send_text(from_wa,
                                 "¿Qué querés hacer?\n\n"
                                 "1) Registrar gasto\n"
                                 "2) Consultar historial\n\n"
                                 "Escribí 1 o 2.")
                    goto("await_menu")
                    continue

                # Selección del menú
                ut = normalize_text(user_text)
                if state == "await_menu":
                    if ut in ("1", "registrar", "registrar gasto"):
                        data_acc.clear()
                        data_acc["momento"] = now_local()
                        wa_send_text(from_wa, f"Vamos a registrar un gasto.\n¿Monto y moneda? (ej: 12500 ARS). Moneda por defecto: {MONEDA_DEFAULT}")
                        goto("await_monto")
                    elif ut in ("2", "consultar", "consultar historial"):
                        wa_send_text(from_wa,
                                     "Consulta de historial. Decime rango de fechas.\n"
                                     "Ejemplos:\n"
                                     "- desde 2025-08-01 hasta 2025-08-31\n"
                                     "- desde 01/08/2025 hasta 31/08/2025\n"
                                     "o escribí 'todo'.")
                        goto("await_hist_rango")
                    else:
                        wa_send_text(from_wa, "No entendí. Escribí 1 o 2.")
                    continue

                # Registrar gasto
                if state == "await_monto":
                    monto, moneda = parse_monto_moneda(user_text)
                    if monto is None:
                        wa_send_text(from_wa, "No pude leer el monto. Probá con '12500' o '12500 ARS'.")
                        continue
                    data_acc["monto"] = monto
                    data_acc["moneda"] = moneda
                    wa_send_text(from_wa, "Descripción (opcional). Escribí una frase o 'ninguna'.")
                    goto("await_descripcion")
                    continue

                if state == "await_descripcion":
                    data_acc["descripcion"] = None if ut in ("", "ninguna", "no") else user_text.strip()
                    wa_send_text(from_wa, "Medio de pago: efectivo, debito, credito o transferencia.")
                    goto("await_medio")
                    continue

                if state == "await_medio":
                    if ut not in MEDIOS_VALIDOS:
                        wa_send_text(from_wa, f"Medio inválido. Usá: {', '.join(MEDIOS_VALIDOS)}")
                        continue
                    data_acc["medio_pago"] = ut
                    wa_send_text(from_wa, "Banco (opcional). Ej: GALICIA / BBVA / NACION… o 'ninguno'.")
                    goto("await_banco")
                    continue

                if state == "await_banco":
                    data_acc["banco"] = None if ut in ("", "ninguno", "no") else user_text.strip()
                    if data_acc.get("medio_pago") in ("debito", "credito"):
                        wa_send_text(from_wa, "Marca de tarjeta (opcional). Ej: Visa, Mastercard, Amex, Cabal… o 'ninguna'.")
                        goto("await_marca")
                    else:
                        wa_send_text(from_wa, "Cuenta/Alias (opcional). Escribí 'ninguna' si no aplica.")
                        goto("await_cuenta")
                    continue

                if state == "await_marca":
                    data_acc["marca_tarjeta"] = None if ut in ("", "ninguna", "no") else user_text.strip()
                    wa_send_text(from_wa, "Cuenta/Alias (opcional). Escribí 'ninguna' si no aplica.")
                    goto("await_cuenta")
                    continue

                if state == "await_cuenta":
                    data_acc["cuenta_pago"] = None if ut in ("", "ninguna", "no") else user_text.strip()
                    wa_send_text(from_wa, "Categoría (opcional). Ej: Comida, Transporte, Servicios… o 'ninguna'.")
                    goto("await_categoria")
                    continue

                if state == "await_categoria":
                    data_acc["categoria"] = None if ut in ("", "ninguna", "no") else user_text.strip()
                    wa_send_text(from_wa, "Comercio/Proveedor (opcional). Escribí 'ninguno' si no aplica.")
                    goto("await_comercio")
                    continue

                if state == "await_comercio":
                    data_acc["comercio"] = None if ut in ("", "ninguno", "no") else user_text.strip()
                    # Confirmar e insertar
                    resumen = [
                        f"Monto: {data_acc.get('monto')} {data_acc.get('moneda', MONEDA_DEFAULT)}",
                        f"Descripción: {data_acc.get('descripcion') or '-'}",
                        f"Medio: {data_acc.get('medio_pago')}",
                        f"Banco: {data_acc.get('banco') or '-'}",
                        f"Marca: {data_acc.get('marca_tarjeta') or '-'}",
                        f"Cuenta: {data_acc.get('cuenta_pago') or '-'}",
                        f"Categoría: {data_acc.get('categoria') or '-'}",
                        f"Comercio: {data_acc.get('comercio') or '-'}",
                    ]
                    wa_send_text(from_wa, "Registrando gasto…\n" + "\n".join(resumen))
                    try:
                        clave = await insert_gasto(data_acc, texto_original=user_text, origen=from_wa)
                        wa_send_text(from_wa, f"✅ Gasto registrado (clave {clave}). ¡Gracias!")
                    except Exception as e:
                        log.exception("Error insertando gasto: %s", e)
                        wa_send_text(from_wa, "No pude registrar el gasto. Revisá los datos e intentá de nuevo.")
                    goto("menu")
                    wa_send_text(from_wa, "¿Querés hacer algo más? Escribí 'menu'.")
                    continue

                # Consultar historial
                if state == "await_hist_rango":
                    if ut == "todo":
                        fd = fh = None
                    else:
                        txt = (user_text or "").lower()
                        fd = fh = None
                        if "desde" in txt and "hasta" in txt:
                            try:
                                parte_desde = txt.split("desde", 1)[1].split("hasta", 1)[0].strip()
                                parte_hasta = txt.split("hasta", 1)[1].strip()
                                fd = parse_fecha_hora_local(parte_desde)
                                fh = parse_fecha_hora_local(parte_hasta)
                            except Exception:
                                pass
                    data_acc["fd"] = fd
                    data_acc["fh"] = fh
                    wa_send_text(from_wa,
                                 "Si querés filtrar por medio, escribí uno de: "
                                 f"{', '.join(MEDIOS_VALIDOS)}.\n"
                                 "O escribí 'ninguno' para no filtrar.")
                    goto("await_hist_medio")
                    continue

                if state == "await_hist_medio":
                    medio = None if ut in ("", "ninguno", "no") else ut
                    if medio and medio not in MEDIOS_VALIDOS:
                        wa_send_text(from_wa, f"Medio inválido. Usá: {', '.join(MEDIOS_VALIDOS)}")
                        continue
                    fd = data_acc.get("fd")
                    fh = data_acc.get("fh")
                    res = await resumen_historial(fd, fh, medio)
                    wa_send_text(from_wa, "Resumen:\n" + res)
                    goto("menu")
                    wa_send_text(from_wa, "¿Querés hacer algo más? Escribí 'menu'.")
                    continue

                # Cualquier otro estado: volver a menú
                goto("menu")
                wa_send_text(from_wa, "Volvamos al menú. Escribí 'menu'.")

    except Exception as e:
        log.exception("Error procesando evento: %s", e)

    return JSONResponse({"status": "ok"})

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.getenv("PORT", "8000")))
