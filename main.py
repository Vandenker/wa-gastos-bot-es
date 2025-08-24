
import os, re, json, hmac, hashlib, datetime as dt, pytz, requests
from fastapi import FastAPI, Request, Header, HTTPException
from fastapi.responses import PlainTextResponse
from sqlalchemy import create_engine, text

# === ENTORNO ===
WHATSAPP_TOKEN  = os.getenv("WHATSAPP_TOKEN")
PHONE_NUMBER_ID = os.getenv("PHONE_NUMBER_ID")
APP_SECRET      = os.getenv("APP_SECRET")
DATABASE_URL    = os.getenv("DATABASE_URL")
VERIFY_TOKEN    = os.getenv("VERIFY_TOKEN", "miverify123")
TZ              = os.getenv("TZ", "America/Argentina/Buenos_Aires")

if not all([WHATSAPP_TOKEN, PHONE_NUMBER_ID, APP_SECRET, DATABASE_URL]):
    raise RuntimeError("Faltan variables de entorno necesarias.")

engine = create_engine(DATABASE_URL, pool_pre_ping=True)
app = FastAPI(title="Gastos Bot ES")

TIMEOUT_MIN = 2
MAX_OPS = 5

# === UTILIDADES ===
def now_local():
    return dt.datetime.now(pytz.timezone(TZ))

def enviar_whatsapp(to: str, body: str):
    url = f"https://graph.facebook.com/v20.0/{PHONE_NUMBER_ID}/messages"
    headers = {"Authorization": f"Bearer {WHATSAPP_TOKEN}", "Content-Type": "application/json"}
    payload = {"messaging_product": "whatsapp", "to": to, "text": {"body": body[:4000]}}
    r = requests.post(url, headers=headers, json=payload, timeout=20)
    r.raise_for_status()

def verificar_firma(raw_body: bytes, sig: str | None):
    if not sig: raise HTTPException(status_code=401, detail="Falta firma")
    mac = hmac.new(APP_SECRET.encode(), msg=raw_body, digestmod=hashlib.sha256).hexdigest()
    if not hmac.compare_digest("sha256="+mac, sig):
        raise HTTPException(status_code=401, detail="Firma inválida")

def normalizar_si_no(txt: str):
    t = txt.strip().lower()
    if t in ("si","sí","s","ok","dale","claro","afirmativo","yes","y"): return "si"
    if t in ("no","n","nope"): return "no"
    return None

def normalizar_ahora_otro(txt: str):
    t = txt.strip().lower()
    if "ahora" in t: return "ahora"
    if "otro" in t or "antes" in t: return "otro"
    return None

def parse_fecha(txt: str):
    m = re.search(r"(\d{4})-(\d{2})-(\d{2})", txt)
    if not m: return None
    try:
        return dt.date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
    except: return None

def parse_hora(txt: str):
    m = re.search(r"\b([01]?\d|2[0-3]):([0-5]\d)\b", txt)
    if not m: return None
    try:
        return dt.time(int(m.group(1)), int(m.group(2)))
    except: return None

def parse_monto_moneda(txt: str):
    m = re.search(r"(\d+(?:[.,]\d{1,2})?)\s*(ars|usd|eur|brl)?", txt, re.IGNORECASE)
    if not m: return None, None
    monto = float(m.group(1).replace(",", "."))
    moneda = m.group(2).upper() if m.group(2) else "ARS"
    return monto, moneda

def medio_pago_valido(txt: str):
    t = txt.strip().lower()
    opciones = {"efectivo","debito","credito","transferencia"}
    return t if t in opciones else None

def session_expirada(actualizado_en: dt.datetime) -> bool:
    if not actualizado_en:
        return True
    ahora = dt.datetime.now(dt.timezone.utc)
    return (ahora - actualizado_en) > dt.timedelta(minutes=TIMEOUT_MIN)

# === CONVERSACIÓN ===
def get_conv(wa: str):
    q = text("select estado, datos_json, actualizado_en from conversaciones where whatsapp=:w")
    with engine.begin() as c:
        row = c.execute(q, {"w": wa}).first()
        if not row: return None, {}, None
        return row.estado, row.datos_json, row.actualizado_en

def set_conv(wa: str, estado: str, datos: dict):
    q1 = text(\"""
        insert into conversaciones (whatsapp, estado, datos_json, actualizado_en)
        values (:w, :e, :d::jsonb, now())
        on conflict (whatsapp) do update
        set estado=excluded.estado, datos_json=excluded.datos_json, actualizado_en=now()
    \""")
    with engine.begin() as c:
        c.execute(q1, {"w": wa, "e": estado, "d": json.dumps(datos, ensure_ascii=False)})

def clear_conv(wa: str):
    with engine.begin() as c:
        c.execute(text("delete from conversaciones where whatsapp=:w"), {"w": wa})

# === IDEMPOTENCIA ===
def visto_o_registrar(mid: str) -> bool:
    if not mid: return False
    with engine.begin() as c:
        r = c.execute(text("select 1 from mensajes_procesados where clave_mensaje=:id"), {"id": mid}).first()
        if r: return True
        c.execute(text("insert into mensajes_procesados(clave_mensaje) values (:id)"), {"id": mid})
        return False

# === CATALOGO ===
def listar_opciones(campo, wa):
    if campo == 'banco':
        sql = "select nombre from catalogo_bancos where whatsapp=:wa order by id desc limit :n"
    elif campo == 'marca':
        sql = "select nombre from catalogo_marcas where whatsapp=:wa order by id desc limit :n"
    else:
        sql = "select nombre from catalogo_categorias where whatsapp=:wa order by id desc limit :n"
    with engine.begin() as c:
        return c.execute(text(sql), {"wa": wa, "n": MAX_OPS}).scalars().all()

def existe_opcion(campo, wa, valor):
    sqls = {
        'banco': "select 1 from catalogo_bancos where whatsapp=:wa and lower(nombre)=lower(:v)",
        'marca': "select 1 from catalogo_marcas where whatsapp=:wa and lower(nombre)=lower(:v)",
        'categoria': "select 1 from catalogo_categorias where whatsapp=:wa and lower(nombre)=lower(:v)"
    }
    with engine.begin() as c:
        return c.execute(text(sqls[campo]), {"wa": wa, "v": valor}).first() is not None

def crear_opcion(campo, wa, valor):
    sqls = {
        'banco': "insert into catalogo_bancos(whatsapp,nombre) values (:wa,:v) on conflict do nothing",
        'marca': "insert into catalogo_marcas(whatsapp,nombre) values (:wa,:v) on conflict do nothing",
        'categoria': "insert into catalogo_categorias(whatsapp,nombre) values (:wa,:v) on conflict do nothing"
    }
    with engine.begin() as c:
        c.execute(text(sqls[campo]), {"wa": wa, "v": valor})

# === PERSISTENCIA DE GASTO ===
def guardar_gasto(datos: dict):
    q = text(\"""
        insert into gastos
        (fecha_hora_utc, monto, moneda, descripcion, categoria, comercio, medio_pago,
         banco, marca_tarjeta, cuenta_pago, texto_original, whatsapp_origen)
        values (:fecha_hora_utc, :monto, :moneda, :descripcion, :categoria, :comercio, :medio_pago,
                :banco, :marca_tarjeta, :cuenta_pago, :texto_original, :whatsapp_origen)
        returning clave
    \""")
    with engine.begin() as c:
        return c.execute(q, datos).scalar()

# === CONSULTAS ===
def consultar_libre(wa_from: str, filtro_txt: str):
    medio = None
    for k in ("credito","debito","efectivo","transferencia"):
        if re.search(rf"\\b{k}\\b", filtro_txt, re.IGNORECASE): medio = k

    marca = None
    m = re.search(r"\\bvisa|mastercard|amex|naranja|cabal\\b", filtro_txt, re.IGNORECASE)
    if m: marca = m.group(0).capitalize()

    banco = None
    m2 = re.search(r"\\b(nacion|bbva|santander|galicia|itau|macro|hsbc|patagonia|credicoop|lemon|mercado pago)\\b", filtro_txt, re.IGNORECASE)
    if m2: banco = m2.group(0).upper()

    f_desde = parse_fecha(filtro_txt)
    f_hasta = None
    m3 = re.search(r"hasta\\s+(\\d{4}-\\d{2}-\\d{2})", filtro_txt, re.IGNORECASE)
    if m3:
        try: y,mn,d = m3.group(1).split("-"); f_hasta = dt.date(int(y), int(mn), int(d))
        except: pass

    params = {"wa": f"+{wa_from}"}
    condiciones = ["whatsapp_origen = :wa"]
    if medio:
        condiciones.append("medio_pago = :medio"); params["medio"] = medio
    if marca:
        condiciones.append("marca_tarjeta ilike :marca"); params["marca"] = f"%{marca}%"
    if banco:
        condiciones.append("banco ilike :banco"); params["banco"] = f"%{banco}%"
    if f_desde:
        tz = pytz.timezone(TZ)
        desde_dt = tz.localize(dt.datetime.combine(f_desde, dt.time(0,0))).astimezone(dt.timezone.utc)
        condiciones.append("fecha_hora_utc >= :desde"); params["desde"] = desde_dt
    if f_hasta:
        tz = pytz.timezone(TZ)
        hasta_dt = tz.localize(dt.datetime.combine(f_hasta+dt.timedelta(days=1), dt.time(0,0))).astimezone(dt.timezone.utc)
        condiciones.append("fecha_hora_utc < :hasta"); params["hasta"] = hasta_dt

    where = " and ".join(condiciones)
    q = text(f\"""
        select fecha_hora_utc, monto, moneda, descripcion, medio_pago, banco, marca_tarjeta, cuenta_pago
        from gastos
        where {where}
        order by fecha_hora_utc desc
        limit 200
    \""")
    with engine.begin() as c:
        rows = c.execute(q, params).all()

    if not rows: return "No encontré movimientos con esos filtros."
    total = sum(float(r.monto) for r in rows)
    tz = pytz.timezone(TZ)
    out = [f"Movimientos: {len(rows)} | Total: {total:.2f} {rows[0].moneda}"]
    for r in rows[:20]:
        fh = r.fecha_hora_utc.astimezone(tz).strftime("%Y-%m-%d %H:%M")
        out.append(f"- {fh} • {r.monto} {r.moneda} • {r.medio_pago} • {r.banco or ''} {r.marca_tarjeta or ''} • {r.descripcion or ''}")
    if len(rows) > 20: out.append("… (mostrando 20 de 200 máx)")
    return "\\n".join(out)

def ejecutar_consulta_guiada(wa_from: str, filtros: dict) -> str:
    condiciones = ["whatsapp_origen = :wa"]
    params = {"wa": f"+{wa_from}"}
    tz = pytz.timezone(TZ)

    f_desde = dt.date.fromisoformat(filtros["desde"])
    f_hasta = dt.date.fromisoformat(filtros["hasta"])
    params["desde"] = tz.localize(dt.datetime.combine(f_desde, dt.time(0,0))).astimezone(dt.timezone.utc)
    params["hasta"] = tz.localize(dt.datetime.combine(f_hasta+dt.timedelta(days=1), dt.time(0,0))).astimezone(dt.timezone.utc)
    condiciones += ["fecha_hora_utc >= :desde", "fecha_hora_utc < :hasta"]

    if filtros.get("medio") and filtros["medio"]!="todos":
        condiciones.append("medio_pago = :medio"); params["medio"] = filtros["medio"]
    if filtros.get("banco"):
        condiciones.append("banco ilike :banco"); params["banco"] = filtros["banco"]

    where = " and ".join(condiciones)
    q = text(f\"""
      select fecha_hora_utc, monto, moneda, descripcion, medio_pago, banco, marca_tarjeta, cuenta_pago
      from gastos
      where {where}
      order by fecha_hora_utc desc
      limit 200
    \""")
    with engine.begin() as c:
        rows = c.execute(q, params).all()

    if not rows: return "No encontré movimientos con esos filtros."
    total = sum(float(r.monto) for r in rows)
    out = [f"Movimientos: {len(rows)} | Total: {total:.2f} {rows[0].moneda}"]
    for r in rows[:20]:
        fh = r.fecha_hora_utc.astimezone(tz).strftime("%Y-%m-%d %H:%M")
        out.append(f"- {fh} • {r.monto} {r.moneda} • {r.medio_pago} • {r.banco or ''} {r.marca_tarjeta or ''} • {r.descripcion or ''}")
    if len(rows) > 20: out.append("… (mostrando 20 de 200 máx)")
    return "\\n".join(out)

# === WEBHOOKS ===
@app.get("/webhook", response_class=PlainTextResponse)
def verify(mode: str="", challenge: str="", token: str=""):
    if mode == "subscribe" and token == VERIFY_TOKEN:
        return challenge
    return PlainTextResponse("forbidden", status_code=403)

@app.post("/webhook")
async def incoming(request: Request, x_hub_signature_256: str | None = Header(None)):
    raw = await request.body()
    verificar_firma(raw, x_hub_signature_256)
    payload = json.loads(raw)

    try:
        value = payload["entry"][0]["changes"][0]["value"]
        messages = value.get("messages", [])
        if not messages: return {"ok": True}
        msg = messages[0]
        mid = msg.get("id") or ""
        from_id = msg["from"]
        texto = msg.get("text",{}).get("body","").strip()
    except Exception:
        return {"ok": True}

    # idempotencia
    if visto_o_registrar(mid): return {"ok": True}

    wa = f"+{from_id}"
    estado, datos, actualizado_en = get_conv(wa)
    t = texto.lower()

    # timeout 2'
    if estado and session_expirada(actualizado_en):
        clear_conv(wa)
        enviar_whatsapp(from_id, "⏱️ Pasaron más de 2 minutos sin respuesta. Escribí *hola* para reiniciar.")
        return {"ok": True}

    # atajos
    if t in ("cancelar","salir","stop"):
        clear_conv(wa)
        enviar_whatsapp(from_id, "Flujo cancelado. Escribí *hola* para volver al menú.")
        return {"ok": True}

    if t.startswith("pasame ") or "desde" in t or "hasta" in t:
        resp = consultar_libre(from_id, texto)
        enviar_whatsapp(from_id, resp)
        return {"ok": True}

    # inicio / menú
    if t in ("hola","menu","menú","hi","inicio","buenas"):
        set_conv(wa, "menu", {})
        enviar_whatsapp(from_id, "¿Qué te gustaría hacer?\\n1) Registrar gasto\\n2) Consultar historial\\n\\nEscribí 1 o 2.")
        return {"ok": True}

    if not estado:
        set_conv(wa, "menu", {})
        enviar_whatsapp(from_id, "¿Qué te gustaría hacer?\\n1) Registrar gasto\\n2) Consultar historial\\n\\nEscribí 1 o 2.")
        return {"ok": True}

    # ===== ROUTER DE ESTADOS =====
    if estado == "menu":
        if t in ("1","registrar","registrar gasto","gasto"):
            set_conv(wa, "confirma_registro", {})
            enviar_whatsapp(from_id, "¿Querés registrar un gasto? (sí/no)")
        elif t in ("2","consultar","consultar historial","historial"):
            set_conv(wa, "consulta_desde", {})
            enviar_whatsapp(from_id, "¿Desde qué fecha? AAAA-MM-DD")
        else:
            enviar_whatsapp(from_id, "No entendí. Escribí 1 (Registrar gasto) o 2 (Consultar historial).")
        return {"ok": True}

    # === CONSULTA GUIADA ===
    if estado == "consulta_desde":
        f = parse_fecha(t)
        if not f: enviar_whatsapp(from_id, "Formato inválido. Ej: 2025-08-01"); return {"ok": True}
        datos["desde"] = f.isoformat()
        set_conv(wa, "consulta_hasta", datos)
        enviar_whatsapp(from_id, "¿Hasta qué fecha? AAAA-MM-DD")
        return {"ok": True}

    if estado == "consulta_hasta":
        f = parse_fecha(t)
        if not f: enviar_whatsapp(from_id, "Formato inválido. Ej: 2025-08-31"); return {"ok": True}
        datos["hasta"] = f.isoformat()
        set_conv(wa, "consulta_medio", datos)
        enviar_whatsapp(from_id, "Medio de pago (número):\\n0) Todos\\n1) efectivo\\n2) debito\\n3) credito\\n4) transferencia")
        return {"ok": True}

    if estado == "consulta_medio":
        mapa = {"1":"efectivo","2":"debito","3":"credito","4":"transferencia","0":"todos"}
        if t in mapa:
            datos["medio"] = mapa[t]
            ops = listar_opciones("banco", wa)
            menu = ["Banco (número):", "0) Todos"] + [f"{i}) {o}" for i,o in enumerate(ops, start=1)]
            menu.append("9) Otra (crear nueva)")
            set_conv(wa, "consulta_banco_menu", datos | {"ops_banco": ops})
            enviar_whatsapp(from_id, "\\n".join(menu))
        else:
            enviar_whatsapp(from_id, "Elegí 0/1/2/3/4")
        return {"ok": True}

    if estado == "consulta_banco_menu":
        ops = datos.get("ops_banco", [])
        if t.isdigit():
            n = int(t)
            if n == 0:
                datos["banco"] = None
            elif n == 9:
                set_conv(wa, "consulta_banco_valor", datos); enviar_whatsapp(from_id, "Escribí el nombre del banco:"); return {"ok": True}
            elif 1 <= n <= len(ops):
                datos["banco"] = ops[n-1]
            else:
                enviar_whatsapp(from_id, "Número no válido."); return {"ok": True}
        else:
            valor = texto.strip()
            if existe_opcion("banco", wa, valor):
                datos["banco"] = valor
            else:
                set_conv(wa, "consulta_banco_confirma", datos | {"pendiente_banco": valor})
                enviar_whatsapp(from_id, f"No existe “{valor}”. ¿Crear? (sí/no)")
                return {"ok": True}

        set_conv(wa, "consulta_confirma", datos)
        medio_txt = datos["medio"] if datos["medio"]!="todos" else "todos"
        banco_txt = datos["banco"] or "todos"
        enviar_whatsapp(from_id, f"Voy a buscar desde {datos['desde']} hasta {datos['hasta']}\\nMedio: {medio_txt}\\nBanco: {banco_txt}\\n\\n¿Confirmo? (sí/no)")
        return {"ok": True}

    if estado == "consulta_banco_valor":
        valor = texto.strip()
        set_conv(wa, "consulta_banco_confirma", datos | {"pendiente_banco": valor})
        enviar_whatsapp(from_id, f"¿Crear banco “{valor}”? (sí/no)")
        return {"ok": True}

    if estado == "consulta_banco_confirma":
        resp = normalizar_si_no(t)
        if resp == "si":
            crear_opcion("banco", wa, datos["pendiente_banco"])
            datos["banco"] = datos["pendiente_banco"]; datos.pop("pendiente_banco", None)
            set_conv(wa, "consulta_confirma", datos)
            medio_txt = datos["medio"] if datos["medio"]!="todos" else "todos"
            banco_txt = datos["banco"] or "todos"
            enviar_whatsapp(from_id, f"Voy a buscar desde {datos['desde']} hasta {datos['hasta']}\\nMedio: {medio_txt}\\nBanco: {banco_txt}\\n\\n¿Confirmo? (sí/no)")
        elif resp == "no":
            ops = listar_opciones("banco", wa)
            menu = ["Banco (número):", "0) Todos"] + [f"{i}) {o}" for i,o in enumerate(ops, start=1)]
            menu.append("9) Otra (crear nueva)")
            set_conv(wa, "consulta_banco_menu", datos | {"ops_banco": ops})
            enviar_whatsapp(from_id, "\\n".join(menu))
        else:
            enviar_whatsapp(from_id, "Respondé sí o no.")
        return {"ok": True}

    if estado == "consulta_confirma":
        resp = normalizar_si_no(t)
        if resp == "si":
            txt = ejecutar_consulta_guiada(from_id, datos); clear_conv(wa); enviar_whatsapp(from_id, txt)
        elif resp == "no":
            clear_conv(wa); enviar_whatsapp(from_id, "Consulta cancelada. Escribí *hola* para volver al menú.")
        else:
            enviar_whatsapp(from_id, "Respondé *sí* o *no*.")
        return {"ok": True}

    # === REGISTRO GUIADO ===
    if estado == "confirma_registro":
        s = normalizar_si_no(t)
        if s == "si":
            set_conv(wa, "fecha_momento", {}); enviar_whatsapp(from_id, "¿El gasto es de *ahora* o de *otro momento*?")
        elif s == "no":
            clear_conv(wa); enviar_whatsapp(from_id, "Perfecto, ¡gracias!")
        else:
            enviar_whatsapp(from_id, "Respondé *sí* o *no*.")
        return {"ok": True}

    if estado == "fecha_momento":
        a = normalizar_ahora_otro(t)
        if a == "ahora":
            datos["fecha_hora_utc"] = now_local().astimezone(dt.timezone.utc).isoformat()
            set_conv(wa, "pide_monto", datos); enviar_whatsapp(from_id, "Decime *monto y moneda*. Ej: 4500 ARS")
        elif a == "otro":
            set_conv(wa, "pide_fecha", datos); enviar_whatsapp(from_id, "Indicá la *fecha* (AAAA-MM-DD).")
        else:
            enviar_whatsapp(from_id, "Decime: *ahora* o *otro momento*.")
        return {"ok": True}

    if estado == "pide_fecha":
        f = parse_fecha(t)
        if not f: enviar_whatsapp(from_id, "Formato inválido (AAAA-MM-DD)."); return {"ok": True}
        datos["fecha_parcial"] = f.isoformat()
        set_conv(wa, "pide_hora", datos); enviar_whatsapp(from_id, "Indicá la *hora* (HH:MM 24h).")
        return {"ok": True}

    if estado == "pide_hora":
        h = parse_hora(t)
        if not h: enviar_whatsapp(from_id, "Hora inválida (HH:MM)."); return {"ok": True}
        zona = pytz.timezone(TZ)
        dt_local = zona.localize(dt.datetime.combine(dt.date.fromisoformat(datos["fecha_parcial"]), h))
        datos["fecha_hora_utc"] = dt_local.astimezone(dt.timezone.utc).isoformat()
        datos.pop("fecha_parcial", None)
        set_conv(wa, "pide_monto", datos); enviar_whatsapp(from_id, "Decime *monto y moneda*. Ej: 4500 ARS")
        return {"ok": True}

    if estado == "pide_monto":
        monto, moneda = parse_monto_moneda(texto)
        if not monto: enviar_whatsapp(from_id, "No pude leer el monto/moneda. Ej: 4500 ARS"); return {"ok": True}
        datos.update({"monto": monto, "moneda": moneda})
        set_conv(wa, "pide_descripcion", datos); enviar_whatsapp(from_id, "Una *descripción* breve (ej: supermercado coto):")
        return {"ok": True}

    if estado == "pide_descripcion":
        datos["descripcion"] = texto.strip()
        set_conv(wa, "pide_medio_pago", datos)
        enviar_whatsapp(from_id, "Medio de pago: *efectivo*, *debito*, *credito* o *transferencia*?")
        return {"ok": True}

    if estado == "pide_medio_pago":
        mp = medio_pago_valido(t)
        if not mp: enviar_whatsapp(from_id, "Elegí: efectivo / debito / credito / transferencia"); return {"ok": True}
        datos["medio_pago"] = mp
        # menú de banco
        ops = listar_opciones("banco", wa)
        menu = ["Banco (número):", "0) Ninguno"] + [f"{i}) {o}" for i,o in enumerate(ops, start=1)]
        menu.append("9) Otra (crear nueva)")
        set_conv(wa, "banco_menu", datos | {"ops_banco": ops})
        enviar_whatsapp(from_id, "\\n".join(menu))
        return {"ok": True}

    if estado == "banco_menu":
        ops = datos.get("ops_banco", [])
        if t.isdigit():
            n = int(t)
            if n == 0:
                datos["banco"] = None
            elif n == 9:
                set_conv(wa, "banco_valor", datos); enviar_whatsapp(from_id, "Escribí el nombre del banco:"); return {"ok": True}
            elif 1 <= n <= len(ops):
                datos["banco"] = ops[n-1]
            else:
                enviar_whatsapp(from_id, "Número no válido."); return {"ok": True}
        else:
            valor = texto.strip()
            if existe_opcion("banco", wa, valor):
                datos["banco"] = valor
            else:
                set_conv(wa, "banco_confirma_crear", datos | {"pendiente_banco": valor})
                enviar_whatsapp(from_id, f"No existe “{valor}”. ¿Crear? (sí/no)")
                return {"ok": True}

        # menú de marca
        opsm = listar_opciones("marca", wa)
        menum = ["Marca (número):", "0) Ninguna"] + [f"{i}) {o}" for i,o in enumerate(opsm, start=1)]
        menum.append("9) Otra (crear nueva)")
        set_conv(wa, "marca_menu", datos | {"ops_marca": opsm})
        enviar_whatsapp(from_id, "\\n".join(menum))
        return {"ok": True}

    if estado == "banco_valor":
        valor = texto.strip()
        set_conv(wa, "banco_confirma_crear", datos | {"pendiente_banco": valor})
        enviar_whatsapp(from_id, f"¿Crear banco “{valor}”? (sí/no)")
        return {"ok": True}

    if estado == "banco_confirma_crear":
        resp = normalizar_si_no(t)
        if resp == "si":
            crear_opcion("banco", wa, datos["pendiente_banco"])
            datos["banco"] = datos["pendiente_banco"]; datos.pop("pendiente_banco", None)
            opsm = listar_opciones("marca", wa)
            menum = ["Marca (número):", "0) Ninguna"] + [f"{i}) {o}" for i,o in enumerate(opsm, start=1)]
            menum.append("9) Otra (crear nueva)")
            set_conv(wa, "marca_menu", datos | {"ops_marca": opsm})
            enviar_whatsapp(from_id, "\\n".join(menum))
        elif resp == "no":
            ops = listar_opciones("banco", wa)
            menu = ["Banco (número):", "0) Ninguno"] + [f"{i}) {o}" for i,o in enumerate(ops, start=1)]
            menu.append("9) Otra (crear nueva)")
            set_conv(wa, "banco_menu", datos | {"ops_banco": ops})
            enviar_whatsapp(from_id, "\\n".join(menu))
        else:
            enviar_whatsapp(from_id, "Respondé sí o no.")
        return {"ok": True}

    if estado == "marca_menu":
        ops = datos.get("ops_marca", [])
        if t.isdigit():
            n = int(t)
            if n == 0:
                datos["marca_tarjeta"] = None
            elif n == 9:
                set_conv(wa, "marca_valor", datos); enviar_whatsapp(from_id, "Escribí la marca (Visa, Mastercard, etc.):"); return {"ok": True}
            elif 1 <= n <= len(ops):
                datos["marca_tarjeta"] = ops[n-1]
            else:
                enviar_whatsapp(from_id, "Número no válido."); return {"ok": True}
        else:
            valor = texto.strip()
            if existe_opcion("marca", wa, valor):
                datos["marca_tarjeta"] = valor
            else:
                set_conv(wa, "marca_confirma_crear", datos | {"pendiente_marca": valor})
                enviar_whatsapp(from_id, f"No existe “{valor}”. ¿Crear? (sí/no)")
                return {"ok": True}

        set_conv(wa, "pide_cuenta", datos); enviar_whatsapp(from_id, "Cuenta/alias/últimos 4 (o escribí 'ninguno'):")
        return {"ok": True}

    if estado == "marca_valor":
        valor = texto.strip()
        set_conv(wa, "marca_confirma_crear", datos | {"pendiente_marca": valor})
        enviar_whatsapp(from_id, f"¿Crear marca “{valor}”? (sí/no)")
        return {"ok": True}

    if estado == "marca_confirma_crear":
        resp = normalizar_si_no(t)
        if resp == "si":
            crear_opcion("marca", wa, datos["pendiente_marca"])
            datos["marca_tarjeta"] = datos["pendiente_marca"]; datos.pop("pendiente_marca", None)
            set_conv(wa, "pide_cuenta", datos); enviar_whatsapp(from_id, "Cuenta/alias/últimos 4 (o 'ninguno'):")
        elif resp == "no":
            opsm = listar_opciones("marca", wa)
            menum = ["Marca (número):", "0) Ninguna"] + [f"{i}) {o}" for i,o in enumerate(opsm, start=1)]
            menum.append("9) Otra (crear nueva)")
            set_conv(wa, "marca_menu", datos | {"ops_marca": opsm})
            enviar_whatsapp(from_id, "\\n".join(menum))
        else:
            enviar_whatsapp(from_id, "Respondé sí o no.")
        return {"ok": True}

    if estado == "pide_cuenta":
        datos["cuenta_pago"] = None if t in ("ninguno","na","n/a","no") else texto.strip()
        datos.setdefault("categoria", None); datos.setdefault("comercio", None)
        datos["texto_original"] = "registrado via dialogo"; datos["whatsapp_origen"] = wa
        if "fecha_hora_utc" not in datos:
            datos["fecha_hora_utc"] = now_local().astimezone(dt.timezone.utc).isoformat()

        clave = guardar_gasto({
            "fecha_hora_utc": dt.datetime.fromisoformat(datos["fecha_hora_utc"]),
            "monto": datos["monto"], "moneda": datos["moneda"],
            "descripcion": datos.get("descripcion"),
            "categoria": datos.get("categoria"), "comercio": datos.get("comercio"),
            "medio_pago": datos.get("medio_pago"), "banco": datos.get("banco"),
            "marca_tarjeta": datos.get("marca_tarjeta"), "cuenta_pago": datos.get("cuenta_pago"),
            "texto_original": datos.get("texto_original"), "whatsapp_origen": datos.get("whatsapp_origen")
        })
        clear_conv(wa); enviar_whatsapp(from_id, f"✅ Gasto registrado (clave {clave}). ¡Gracias!")
        return {"ok": True}

    # fallback
    enviar_whatsapp(from_id, "Escribí *hola* para ver el menú, o probá:\\n\\\"pasame los pagos de tarjeta visa nacion desde 2025-08-01 hasta 2025-08-31\\\"")
    return {"ok": True}

@app.get("/")
def root():
    return {"status": "ok"}
