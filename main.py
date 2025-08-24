# main.py
import os
import hmac
import hashlib
import pytz
from datetime import datetime, timedelta
from typing import Dict, Any, Optional

import requests
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse, PlainTextResponse

from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text


# =========================
# Configuración / Entorno
# =========================

WHATSAPP_TOKEN = os.environ.get("WHATSAPP_TOKEN", "").strip()
PHONE_NUMBER_ID = os.environ.get("PHONE_NUMBER_ID", "").strip()
APP_SECRET = os.environ.get("APP_SECRET", "").strip()

VERIFY_TOKEN = os.environ.get("VERIFY_TOKEN", "miverify123").strip()
TZ_NAME = os.environ.get("TZ", "America/Argentina/Buenos_Aires").strip()
GRAPH_API_VERSION = os.environ.get("GRAPH_API_VERSION", "v20.0").strip()

# Debe tener formato: postgresql+psycopg://USER:PASS@HOST/DB?sslmode=require
DATABASE_URL = os.environ.get("DATABASE_URL", "").strip()

if not (WHATSAPP_TOKEN and PHONE_NUMBER_ID and APP_SECRET and DATABASE_URL):
    # No levantamos excepción para que el healthcheck funcione en Render,
    # pero lo dejamos en logs para que puedas revisarlo.
    print("⚠️ Faltan variables de entorno obligatorias: "
          "WHATSAPP_TOKEN / PHONE_NUMBER_ID / APP_SECRET / DATABASE_URL")

# Motor de BD
engine = create_async_engine(DATABASE_URL, pool_pre_ping=True, future=True)

# Zona horaria local
ARG_TZ = pytz.timezone(TZ_NAME)

# FastAPI
app = FastAPI(title="WA Gastos Bot (ES)", version="1.0")

# Estado de sesión simple en memoria (por número de usuario)
# Por simplicidad (free tier), sin Redis. Expira a los 2 minutos de inactividad.
SESSIONS: Dict[str, Dict[str, Any]] = {}

# Opciones del flujo
MEDIOS_VALIDOS = ["efectivo", "debito", "credito", "transferencia"]
MONEDA_DEFAULT = "ARS"


# =========================
# Utilidades
# =========================

def now_local() -> datetime:
    return datetime.now(ARG_TZ)


def normalize(s: str) -> str:
    return (s or "").strip().lower()


def parse_monto_moneda(s: str) -> (Optional[float], str):
    """
    Intenta extraer un monto (float) y moneda (string) de un texto.
    Ej: "12500", "12500 ars", "12.500 ARS", "1.250,55 Ars"
    """
    raw = s.strip().replace(".", "").replace(",", ".")
    parts = raw.split()
    if not parts:
        return None, MONEDA_DEFAULT

    try:
        monto = float(parts[0])
    except Exception:
        return None, MONEDA_DEFAULT

    moneda = MONEDA_DEFAULT
    if len(parts) > 1:
        moneda = parts[1].upper()
    else:
        moneda = MONEDA_DEFAULT
    return monto, moneda


def parse_fecha_hora(s: str) -> Optional[datetime]:
    """
    Acepta "YYYY-MM-DD HH:MM" o "DD/MM/YYYY HH:MM" (también sin hora).
    Devuelve datetime con TZ local.
    """
    s = s.strip()
    fmts = ["%Y-%m-%d %H:%M", "%d/%m/%Y %H:%M", "%Y-%m-%d", "%d/%m/%Y"]
    for fmt in fmts:
        try:
            dt_naive = datetime.strptime(s, fmt)
            return ARG_TZ.localize(dt_naive)
        except Exception:
            continue
    return None


def session(user: str) -> Dict[str, Any]:
    """Obtiene/crea sesión y aplica timeout de 2 minutos."""
    sess = SESSIONS.get(user)
    now = now_local()
    if not sess or sess.get("expires_at") < now:
        sess = {"state": "menu", "data": {}, "expires_at": now + timedelta(minutes=2)}
        SESSIONS[user] = sess
    else:
        sess["expires_at"] = now + timedelta(minutes=2)
    return sess


def wa_send_text(to: str, body: str) -> None:
    url = f"https://graph.facebook.com/{GRAPH_API_VERSION}/{PHONE_NUMBER_ID}/messages"
    payload = {
        "messaging_product": "whatsapp",
        "to": to,
        "type": "text",
        "text": {"body": body[:4000]},
    }
    headers = {"Authorization": f"Bearer {WHATSAPP_TOKEN}", "Content-Type": "application/json"}
    r = requests.post(url, headers=headers, json=payload, timeout=30)
    if r.status_code >= 300:
        print("❌ Error enviando mensaje:", r.status_code, r.text)


async def insert_mensaje_procesado(msg_id: str) -> bool:
    """
    Devuelve True si se insertó (no existía), False si ya estaba.
    Si la tabla no existe, simplemente lo ignora y devuelve True.
    """
    try:
        q = text("""
            INSERT INTO mensajes_procesados (clave_mensaje)
            VALUES (:m)
            ON CONFLICT (clave_mensaje) DO NOTHING
        """)
        async with engine.begin() as conn:
            await conn.execute(q, {"m": msg_id})
        return True
    except Exception as e:
        # Si no existe la tabla, continuamos (modo permissive)
        print("⚠️ No se pudo registrar mensaje procesado (posible tabla faltante):", e)
        return True


async def insert_gasto(data: Dict[str, Any], texto_original: str, origen: str) -> int:
    """
    Inserta en la tabla `gastos` usando las columnas definidas en tu esquema.
    Devuelve la clave insertada.
    """
    q = text("""
        INSERT INTO gastos
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


async def consulta_historial(fecha_desde: Optional[datetime], fecha_hasta: Optional[datetime],
                             medio: Optional[str]) -> str:
    """
    Devuelve un resumen simple del rango (total y conteo).
    """
    conds = []
    params = {}
    if fecha_desde:
        conds.append("fecha_hora_utc >= :fd")
        params["fd"] = fecha_desde.astimezone(pytz.UTC)
    if fecha_hasta:
        conds.append("fecha_hora_utc <= :fh")
        params["fh"] = fecha_hasta.astimezone(pytz.UTC)
    if medio:
        conds.append("medio_pago = :mp")
        params["mp"] = medio

    where = ""
    if conds:
        where = "WHERE " + " AND ".join(conds)

    q = text(f"""
        SELECT COALESCE(SUM(monto),0) AS total, COUNT(*) AS n
        FROM gastos
        {where}
    """)
    try:
        async with engine.begin() as conn:
            res = await conn.execute(q, params)
            row = res.fetchone()
            total = float(row[0]) if row and row[0] is not None else 0.0
            n = int(row[1]) if row and row[1] is not None else 0
        return f"Movimientos: {n}\nTotal: {total:.2f}"
    except Exception as e:
        print("❌ Error consultando historial:", e)
        return "No pude consultar ahora. Intentá más tarde."


# =========================
# Healthcheck
# =========================

@app.get("/")
async def root():
    return {"status": "ok"}


# =========================
# Webhook (verify + receive)
# =========================

def verify_signature(app_secret: str, request_body: bytes, header_signature: str) -> bool:
    """Valida X-Hub-Signature-256 de Meta."""
    if not header_signature or not header_signature.startswith("sha256="):
        return False
    try:
        their_sig = header_signature.split("=", 1)[1]
        mac = hmac.new(app_secret.encode(), msg=request_body, digestmod=hashlib.sha256)
        expected = mac.hexdigest()
        return hmac.compare_digest(their_sig, expected)
    except Exception:
        return False


@app.get("/webhook")
async def verify(request: Request):
    """
    Verificación inicial (GET) de WhatsApp.
    Meta envía: hub.mode, hub.verify_token, hub.challenge
    """
    params = dict(request.query_params)
    mode = params.get("hub.mode")
    token = params.get("hub.verify_token")
    challenge = params.get("hub.challenge")

    if mode == "subscribe" and token == VERIFY_TOKEN:
        return PlainTextResponse(challenge or "")
    raise HTTPException(status_code=403, detail="Verification failed")


@app.post("/webhook")
async def webhook(request: Request):
    raw = await request.body()

    # Validación de firma (recomendado por Meta)
    sig = request.headers.get("x-hub-signature-256", "")
    if not verify_signature(APP_SECRET, raw, sig):
        print("⚠️ Firma inválida del webhook")
        raise HTTPException(status_code=401, detail="Invalid signature")

    data = await request.json()
    # Estructura: entry[0].changes[0].value.messages[0]
    try:
        entry = data["entry"][0]
        change = entry["changes"][0]
        value = change["value"]
        messages = value.get("messages", [])
        if not messages:
            return JSONResponse({"status": "no message"})
        msg = messages[0]
        msg_id = msg.get("id")
        from_wa = msg.get("from")  # número del usuario
        msg_type = msg.get("type")
    except Exception:
        return JSONResponse({"status": "ignored"})

    # De-duplicación
    ok = await insert_mensaje_procesado(msg_id)
    if not ok:
        return JSONResponse({"status": "duplicate"})

    # Texto que escribió el usuario
    user_txt = ""
    if msg_type == "text":
        user_txt = msg.get("text", {}).get("body", "")
    elif msg_type == "interactive":
        # Quick replies o list replies, normalizamos a texto
        inter = msg.get("interactive", {})
        if inter.get("type") == "button_reply":
            user_txt = inter["button_reply"].get("title", "")
        elif inter.get("type") == "list_reply":
            user_txt = inter["list_reply"].get("title", "")
    else:
        user_txt = ""

    user_txt_norm = normalize(user_txt)

    # Flujo conversacional
    sess = session(from_wa)
    state = sess["state"]
    data_acc = sess["data"]

    def goto(new_state: str):
        sess["state"] = new_state
        sess["expires_at"] = now_local() + timedelta(minutes=2)

    # Entrada al menú
    if state == "menu":
        wa_send_text(
            from_wa,
            "¡Hola! ¿Qué querés hacer?\n\n"
            "1) Registrar gasto\n"
            "2) Consultar historial\n\n"
            "Escribí 1 o 2."
        )
        goto("await_menu")
        return JSONResponse({"status": "ok"})

    # Selección del menú
    if state == "await_menu":
        if user_txt_norm in ("1", "registrar", "registrar gasto"):
            data_acc.clear()
            data_acc["momento"] = now_local()
            wa_send_text(from_wa,
                         "Vamos a registrar un gasto.\n"
                         "¿Monto y moneda? (ej: 12500 ARS)\n"
                         f"Moneda por defecto: {MONEDA_DEFAULT}")
            goto("await_monto")
        elif user_txt_norm in ("2", "consultar", "consultar historial"):
            wa_send_text(from_wa,
                         "Consulta de historial.\n"
                         "Decime rango de fechas.\n"
                         "Ejemplos:\n"
                         "- desde 2025-08-01 hasta 2025-08-31\n"
                         "- desde 01/08/2025 hasta 31/08/2025\n"
                         "o escribí 'todo'.")
            goto("await_hist_rango")
        else:
            wa_send_text(from_wa, "No entendí. Escribí 1 o 2.")
        return JSONResponse({"status": "ok"})

    # === Registrar gasto ===
    if state == "await_monto":
        monto, moneda = parse_monto_moneda(user_txt)
        if monto is None:
            wa_send_text(from_wa, "No pude leer el monto. Probá con algo como '12500' o '12500 ARS'.")
            return JSONResponse({"status": "ok"})
        data_acc["monto"] = monto
        data_acc["moneda"] = moneda
        wa_send_text(from_wa, "Descripción (opcional). Escribí una frase o 'ninguna'.")
        goto("await_descripcion")
        return JSONResponse({"status": "ok"})

    if state == "await_descripcion":
        if user_txt_norm in ("", "ninguna", "no"):
            data_acc["descripcion"] = None
        else:
            data_acc["descripcion"] = user_txt.strip()
        wa_send_text(from_wa,
                     "Medio de pago: elegí una opción o escribí una de estas palabras exactas:\n"
                     f"{', '.join(MEDIOS_VALIDOS)}")
        goto("await_medio")
        return JSONResponse({"status": "ok"})

    if state == "await_medio":
        medio = user_txt_norm
        if medio not in MEDIOS_VALIDOS:
            wa_send_text(from_wa, f"Medio inválido. Usá: {', '.join(MEDIOS_VALIDOS)}")
            return JSONResponse({"status": "ok"})
        data_acc["medio_pago"] = medio
        wa_send_text(from_wa, "Banco (opcional). Ej: GALICIA / BBVA / NACION… o escribí 'ninguno'.")
        goto("await_banco")
        return JSONResponse({"status": "ok"})

    if state == "await_banco":
        data_acc["banco"] = None if user_txt_norm in ("", "ninguno", "no") else user_txt.strip()
        if data_acc.get("medio_pago") in ("debito", "credito"):
            wa_send_text(from_wa, "Marca de tarjeta (opcional). Ej: Visa, Mastercard, Amex, Cabal… o 'ninguna'.")
            goto("await_marca")
        else:
            wa_send_text(from_wa, "Cuenta/Alias (opcional). Escribí 'ninguna' si no aplica.")
            goto("await_cuenta")
        return JSONResponse({"status": "ok"})

    if state == "await_marca":
        data_acc["marca_tarjeta"] = None if user_txt_norm in ("", "ninguna", "no") else user_txt.strip()
        wa_send_text(from_wa, "Cuenta/Alias (opcional). Escribí 'ninguna' si no aplica.")
        goto("await_cuenta")
        return JSONResponse({"status": "ok"})

    if state == "await_cuenta":
        data_acc["cuenta_pago"] = None if user_txt_norm in ("", "ninguna", "no") else user_txt.strip()
        wa_send_text(from_wa, "Categoría (opcional). Ej: Comida, Transporte, Servicios… o 'ninguna'.")
        goto("await_categoria")
        return JSONResponse({"status": "ok"})

    if state == "await_categoria":
        data_acc["categoria"] = None if user_txt_norm in ("", "ninguna", "no") else user_txt.strip()
        wa_send_text(from_wa, "Comercio/Proveedor (opcional). Escribí 'ninguno' si no aplica.")
        goto("await_comercio")
        return JSONResponse({"status": "ok"})

    if state == "await_comercio":
        data_acc["comercio"] = None if user_txt_norm in ("", "ninguno", "no") else user_txt.strip()

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
            clave = await insert_gasto(data_acc, texto_original=user_txt, origen=from_wa)
            wa_send_text(from_wa, f"✅ Gasto registrado (clave {clave}). ¡Gracias!")
        except Exception as e:
            print("❌ Error insertando gasto:", e)
            wa_send_text(from_wa, "No pude registrar el gasto. Verificá los datos y probá de nuevo.")

        # Volvemos a menú
        goto("menu")
        wa_send_text(from_wa, "¿Querés hacer algo más? Escribí 'menu'.")
        return JSONResponse({"status": "ok"})

    # === Consultar historial ===
    if state == "await_hist_rango":
        if user_txt_norm == "todo":
            fd = fh = None
        else:
            # Parseo simple: busca "desde X hasta Y"
            text_low = user_txt.lower()
            fd = fh = None
            if "desde" in text_low and "hasta" in text_low:
                try:
                    parte_desde = text_low.split("desde", 1)[1].split("hasta", 1)[0].strip()
                    parte_hasta = text_low.split("hasta", 1)[1].strip()
                    fd = parse_fecha_hora(parte_desde)
                    fh = parse_fecha_hora(parte_hasta)
                except Exception:
                    pass
        data_acc["fd"] = fd
        data_acc["fh"] = fh
        wa_send_text(from_wa,
                     "Si querés filtrar por medio de pago, escribí uno de estos: "
                     f"{', '.join(MEDIOS_VALIDOS)}.\n"
                     "O escribí 'ninguno' para no filtrar.")
        goto("await_hist_medio")
        return JSONResponse({"status": "ok"})

    if state == "await_hist_medio":
        medio = None if user_txt_norm in ("", "ninguno", "no") else user_txt_norm
        if medio and medio not in MEDIOS_VALIDOS:
            wa_send_text(from_wa, f"Medio inválido. Usá: {', '.join(MEDIOS_VALIDOS)}")
            return JSONResponse({"status": "ok"})

        fd = data_acc.get("fd")
        fh = data_acc.get("fh")
        resumen = await consulta_historial(fd, fh, medio)
        wa_send_text(from_wa, "Resumen:\n" + resumen)
        goto("menu")
        wa_send_text(from_wa, "¿Querés hacer algo más? Escribí 'menu'.")
        return JSONResponse({"status": "ok"})

    # Si cae acá, forzamos menú de nuevo
    goto("menu")
    wa_send_text(from_wa, "Volvamos al menú. Escribí 'menu'.")
    return JSONResponse({"status": "ok"})