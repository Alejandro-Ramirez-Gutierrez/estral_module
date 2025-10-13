# routers/fabricacion_mensual_partidas.py
from fastapi import APIRouter, Request, Query, Cookie
from fastapi.responses import JSONResponse, HTMLResponse
from fastapi.templating import Jinja2Templates
from utils.auth import verificar_access_token
from services.db_service import ejecutar_consulta_sql
from datetime import datetime, timedelta

router = APIRouter()
templates = Jinja2Templates(directory="templates")

# ---------------- CONFIG ----------------
AREAS_PERMITIDAS = [20, 22]
EMPLEADOS_PERMITIDOS = [8811, 8661, 8870, 8740, 4, 5]

# Definimos qué áreas consideramos "productivas" (se suman a totales)
AREAS_PRODUCTIVAS = ("HABILITADO","PERFILADO","ENSAMBLE","CIMSA/ ENSAMBLE")
# Áreas de acabado / retrabajo (no se suman a totales, solo se muestran por separado)
AREAS_ACABADO = ("PINTURA","CIMSA/ PINTURA","GALVANIZADO")

# ---------------- HELPERS ----------------
def validar_token(access_token: str):
    if not access_token:
        return None
    token = access_token.replace("Bearer ", "")
    payload = verificar_access_token(token)
    if not payload:
        return None
    if (payload.get("K_Area") in AREAS_PERMITIDAS) or (payload.get("K_Empleado") in EMPLEADOS_PERMITIDOS):
        return payload
    return None

def get_diaturno_case(col_fecha: str) -> str:
    return f"""
    CASE 
        WHEN Area IN ('ENSAMBLE','CIMSA/ ENSAMBLE','PINTURA','CIMSA/ PINTURA','GALVANIZADO') 
            THEN CASE WHEN DATEPART(HOUR, {col_fecha}) >= 7 THEN CONVERT(date, {col_fecha})
                      ELSE DATEADD(DAY, -1, CONVERT(date, {col_fecha})) END
        ELSE CASE WHEN DATEPART(HOUR, {col_fecha}) >= 7 THEN CONVERT(date, {col_fecha})
                  ELSE DATEADD(DAY, -1, CONVERT(date, {col_fecha})) END
    END
    """

def get_turno_case(col_fecha: str) -> str:
    return f"""
    CASE 
        WHEN Area IN ('ENSAMBLE','CIMSA/ ENSAMBLE','PINTURA','CIMSA/ PINTURA','GALVANIZADO') 
            THEN CASE WHEN DATEPART(HOUR, {col_fecha}) BETWEEN 7 AND 18 THEN 'DIA' ELSE 'NOCHE' END
        ELSE CASE WHEN DATEPART(HOUR, {col_fecha}) BETWEEN 7 AND 18 THEN 'DIA' ELSE 'NOCHE' END
    END
    """

def get_bloque_2h(col_fecha: str) -> str:
    return f"""
    CASE 
        WHEN Area IN ('ENSAMBLE','CIMSA/ ENSAMBLE','PINTURA','CIMSA/ PINTURA','GALVANIZADO') THEN
            RIGHT('0' + CAST(((DATEPART(HOUR, {col_fecha}) - 7 + 24) % 24 / 2 * 2 + 7) % 24 AS VARCHAR(2)), 2) 
            + ':00 - ' + 
            RIGHT('0' + CAST(((DATEPART(HOUR, {col_fecha}) - 7 + 24) % 24 / 2 * 2 + 9) % 24 AS VARCHAR(2)), 2) 
            + ':00'
        ELSE
            RIGHT('0' + CAST(((DATEPART(HOUR, {col_fecha}) - 7 + 24) % 24 / 2 * 2 + 7) % 24 AS VARCHAR(2)), 2) 
            + ':00 - ' + 
            RIGHT('0' + CAST(((DATEPART(HOUR, {col_fecha}) - 7 + 24) % 24 / 2 * 2 + 9) % 24 AS VARCHAR(2)), 2) 
            + ':00'
    END
    """

def fecha_col_case() -> str:
    # usamos Hora para las áreas con Hora; Fecha para las otras (manteniendo tu lógica)
    return "CASE WHEN Area IN ('ENSAMBLE','CIMSA/ ENSAMBLE','PINTURA','CIMSA/ PINTURA','GALVANIZADO') THEN Hora ELSE Fecha END"

# ---------------- PAGE ----------------
@router.get("/", response_class=HTMLResponse)
def partidas_page(request: Request, access_token: str = Cookie(None)):
    payload = validar_token(access_token)
    if not payload:
        return JSONResponse(status_code=403, content={"error": "Acceso denegado"})
    today = datetime.today()
    start_default = today.replace(day=1).strftime("%Y-%m-%d")
    end_default = (today + timedelta(days=1)).strftime("%Y-%m-%d")
    return templates.TemplateResponse("fabricacion_mensual_partidas.html", {
        "request": request,
        "usuario": payload.get("sub", "Usuario"),
        "desde": start_default,
        "hasta": end_default
    })

# ---------------- API: RESUMEN (principal) ----------------
@router.get("/api/resumen")
def api_resumen(desde: str = Query(None), hasta: str = Query(None), access_token: str = Cookie(None)):
    """
    Devuelve resumen agrupado por DiaTurno, Turno, Bloque, Area.
    Además calcula totales únicamente para AREAS_PRODUCTIVAS (evita duplicar por pintura/galvanizado).
    """
    payload = validar_token(access_token)
    if not payload:
        return JSONResponse(status_code=403, content={"error": "Acceso denegado"})

    if not hasta:
        hasta = (datetime.today() + timedelta(days=1)).strftime("%Y-%m-%d")
    if not desde:
        dt = datetime.strptime(hasta, "%Y-%m-%d")
        desde = dt.replace(day=1).strftime("%Y-%m-%d")

    fecha_case = fecha_col_case()
    # Query similar a tu original, pero traemos todas las áreas solicitadas
    query = f"""
    SELECT
        {get_diaturno_case('Hora')} AS DiaTurno,
        {get_turno_case('Hora')} AS Turno,
        {get_bloque_2h('Hora')} AS Bloque,
        Area,
        SUM(KgTotal) AS PesoKg,
        SUM(Cantidad) AS Piezas
    FROM Produccion
    WHERE AREA IN ('ENSAMBLE','CIMSA/ ENSAMBLE','HABILITADO','PERFILADO','PINTURA','CIMSA/ PINTURA','GALVANIZADO')
      AND ({fecha_case}) BETWEEN '{desde}' AND '{hasta}'
    GROUP BY {get_diaturno_case('Hora')},{get_turno_case('Hora')},{get_bloque_2h('Hora')},Area
    ORDER BY DiaTurno, Area, Turno;
    """

    rows = ejecutar_consulta_sql(query, fetchall=True) or []

    # Calculamos totales solo para áreas productivas
    total_kg = 0.0
    total_pzas = 0
    resumen = []
    for r in rows:
        peso = float(r.get("PesoKg") or 0)
        pzas = int(r.get("Piezas") or 0)
        area = r.get("Area")
        if area in AREAS_PRODUCTIVAS:
            total_kg += peso
            total_pzas += pzas

        dia = r.get("DiaTurno")
        if hasattr(dia, "strftime"):
            dia = dia.strftime("%Y-%m-%d")
        resumen.append({
            "DiaTurno": dia,
            "Turno": r.get("Turno"),
            "Bloque": r.get("Bloque"),
            "Area": area,
            "PesoKg": peso,
            "Piezas": pzas
        })

    return {"kpis": {"total_kg": total_kg, "total_piezas": total_pzas}, "resumen": resumen}

# ---------------- API: TENDENCIA POR TURNOS (líneas para día vs noche) ----------------
@router.get("/api/tendencia_turnos")
def api_tendencia_turnos(desde: str = Query(None), hasta: str = Query(None), access_token: str = Cookie(None)):
    """
    Retorna sumas por DiaTurno y Turno separando Áreas productivas y áreas de acabado.
    Formato de respuesta:
      { "dias": [...], "dia": [kg...], "noche": [kg...], "dia_acabado": [...], "noche_acabado": [...] }
    """
    payload = validar_token(access_token)
    if not payload:
        return JSONResponse(status_code=403, content={"error": "Acceso denegado"})

    if not hasta:
        hasta = (datetime.today() + timedelta(days=1)).strftime("%Y-%m-%d")
    if not desde:
        dt = datetime.strptime(hasta, "%Y-%m-%d")
        desde = dt.replace(day=1).strftime("%Y-%m-%d")

    fecha_case = fecha_col_case()

    # Agrupamos por DiaTurno y Turno, separando productivas y acabados con SUM CASE
    query = f"""
    SELECT
      {get_diaturno_case('Hora')} AS DiaTurno,
      {get_turno_case('Hora')} AS Turno,
      SUM(CASE WHEN Area IN ('{"','".join(AREAS_PRODUCTIVAS)}') THEN KgTotal ELSE 0 END) AS Kg_Productivo,
      SUM(CASE WHEN Area IN ('{"','".join(AREAS_PRODUCTIVAS)}') THEN Cantidad ELSE 0 END) AS Pz_Productivo,
      SUM(CASE WHEN Area IN ('{"','".join(AREAS_ACABADO)}') THEN KgTotal ELSE 0 END) AS Kg_Acabado,
      SUM(CASE WHEN Area IN ('{"','".join(AREAS_ACABADO)}') THEN Cantidad ELSE 0 END) AS Pz_Acabado
    FROM Produccion
    WHERE AREA IN ('ENSAMBLE','CIMSA/ ENSAMBLE','HABILITADO','PERFILADO','PINTURA','CIMSA/ PINTURA','GALVANIZADO')
      AND ({fecha_case}) BETWEEN '{desde}' AND '{hasta}'
    GROUP BY {get_diaturno_case('Hora')},{get_turno_case('Hora')}
    ORDER BY DiaTurno, Turno;
    """

    rows = ejecutar_consulta_sql(query, fetchall=True) or []

    # Construimos listas por día en orden
    suma_por_dia = {}
    dias_orden = []
    for r in rows:
        dia = r.get("DiaTurno")
        if hasattr(dia, "strftime"):
            dia = dia.strftime("%Y-%m-%d")
        turno = r.get("Turno")
        if dia not in suma_por_dia:
            suma_por_dia[dia] = {"DIA": {"kg":0,"pz":0}, "NOCHE": {"kg":0,"pz":0}, "DIA_AC": {"kg":0,"pz":0}, "NOCHE_AC": {"kg":0,"pz":0}}
            dias_orden.append(dia)
        suma_por_dia[dia][turno]["kg"] = float(r.get("Kg_Productivo") or 0)
        suma_por_dia[dia][turno]["pz"] = int(r.get("Pz_Productivo") or 0)
        # acabados — si el registro tiene Kg_Acabado lo ponemos en los campos de acabado (debe corresponder al mismo turno)
        suma_por_dia[dia][turno.replace("DIA","DIA_AC").replace("NOCHE","NOCHE_AC")] = {
            "kg": float(r.get("Kg_Acabado") or 0),
            "pz": int(r.get("Pz_Acabado") or 0)
        }

    # Asegurar orden consistente
    dias_orden = sorted(list(set(dias_orden)))

    dia_kg = [suma_por_dia[d]["DIA"]["kg"] if d in suma_por_dia else 0 for d in dias_orden]
    noche_kg = [suma_por_dia[d]["NOCHE"]["kg"] if d in suma_por_dia else 0 for d in dias_orden]
    dia_pz = [suma_por_dia[d]["DIA"]["pz"] if d in suma_por_dia else 0 for d in dias_orden]
    noche_pz = [suma_por_dia[d]["NOCHE"]["pz"] if d in suma_por_dia else 0 for d in dias_orden]

    dia_ac_kg = [suma_por_dia[d]["DIA_AC"]["kg"] if d in suma_por_dia else 0 for d in dias_orden]
    noche_ac_kg = [suma_por_dia[d]["NOCHE_AC"]["kg"] if d in suma_por_dia else 0 for d in dias_orden]
    dia_ac_pz = [suma_por_dia[d]["DIA_AC"]["pz"] if d in suma_por_dia else 0 for d in dias_orden]
    noche_ac_pz = [suma_por_dia[d]["NOCHE_AC"]["pz"] if d in suma_por_dia else 0 for d in dias_orden]

    return {
        "dias": dias_orden,
        "dia_kg": dia_kg, "noche_kg": noche_kg,
        "dia_pz": dia_pz, "noche_pz": noche_pz,
        "dia_ac_kg": dia_ac_kg, "noche_ac_kg": noche_ac_kg,
        "dia_ac_pz": dia_ac_pz, "noche_ac_pz": noche_ac_pz
    }

# ---------------- API: TENDENCIA POR BLOQUES (2h) ----------------
@router.get("/api/tendencia_bloques")
def api_tendencia_bloques(desde: str = Query(None), hasta: str = Query(None), access_token: str = Cookie(None)):
    """
    Retorna sumas por Bloque_Horas y Turno (2h blocks). Útil para líneas por bloque.
    Respuesta: { "bloques": [...], "series": { "Productivo": [...], "Acabado": [...] } }
    """
    payload = validar_token(access_token)
    if not payload:
        return JSONResponse(status_code=403, content={"error": "Acceso denegado"})

    if not hasta:
        hasta = (datetime.today() + timedelta(days=1)).strftime("%Y-%m-%d")
    if not desde:
        dt = datetime.strptime(hasta, "%Y-%m-%d")
        desde = dt.replace(day=1).strftime("%Y-%m-%d")

    fecha_case = fecha_col_case()

    query = f"""
    SELECT
        {get_bloque_2h('Hora')} AS Bloque,
        {get_turno_case('Hora')} AS Turno,
        SUM(CASE WHEN Area IN ('{"','".join(AREAS_PRODUCTIVAS)}') THEN KgTotal ELSE 0 END) AS Kg_Productivo,
        SUM(CASE WHEN Area IN ('{"','".join(AREAS_PRODUCTIVAS)}') THEN Cantidad ELSE 0 END) AS Pz_Productivo,
        SUM(CASE WHEN Area IN ('{"','".join(AREAS_ACABADO)}') THEN KgTotal ELSE 0 END) AS Kg_Acabado,
        SUM(CASE WHEN Area IN ('{"','".join(AREAS_ACABADO)}') THEN Cantidad ELSE 0 END) AS Pz_Acabado
    FROM Produccion
    WHERE AREA IN ('ENSAMBLE','CIMSA/ ENSAMBLE','HABILITADO','PERFILADO','PINTURA','CIMSA/ PINTURA','GALVANIZADO')
      AND ({fecha_case}) BETWEEN '{desde}' AND '{hasta}'
    GROUP BY {get_bloque_2h('Hora')},{get_turno_case('Hora')}
    ORDER BY Bloque;
    """

    rows = ejecutar_consulta_sql(query, fetchall=True) or []

    # Construir orden de bloques (usar el texto tal cual)
    bloques_orden = sorted(list({r["Bloque"] for r in rows if r.get("Bloque")}))
    # Mapear sums per bloque (separado día/noche)
    prod_day = []
    prod_night = []
    ac_day = []
    ac_night = []
    for b in bloques_orden:
        # sumar por turno
        kg_day = sum(float(r.get("Kg_Productivo") or 0) for r in rows if r.get("Bloque")==b and r.get("Turno")=="DIA")
        kg_night = sum(float(r.get("Kg_Productivo") or 0) for r in rows if r.get("Bloque")==b and r.get("Turno")=="NOCHE")
        ac_kg_day = sum(float(r.get("Kg_Acabado") or 0) for r in rows if r.get("Bloque")==b and r.get("Turno")=="DIA")
        ac_kg_night = sum(float(r.get("Kg_Acabado") or 0) for r in rows if r.get("Bloque")==b and r.get("Turno")=="NOCHE")

        prod_day.append(kg_day)
        prod_night.append(kg_night)
        ac_day.append(ac_kg_day)
        ac_night.append(ac_kg_night)

    return {"bloques": bloques_orden, "prod_day": prod_day, "prod_night": prod_night, "ac_day": ac_day, "ac_night": ac_night}

# ---------------- API: ACABADOS (porcentaje de piezas que pasan a pintura/galvanizado) ----------------
@router.get("/api/acabados")
def api_acabados(desde: str = Query(None), hasta: str = Query(None), access_token: str = Cookie(None)):
    """
    Retorna totales y porcentaje de piezas que pasan a acabado (pintura/galvanizado)
    """
    payload = validar_token(access_token)
    if not payload:
        return JSONResponse(status_code=403, content={"error": "Acceso denegado"})

    if not hasta:
        hasta = (datetime.today() + timedelta(days=1)).strftime("%Y-%m-%d")
    if not desde:
        dt = datetime.strptime(hasta, "%Y-%m-%d")
        desde = dt.replace(day=1).strftime("%Y-%m-%d")

    fecha_case = fecha_col_case()

    # Totales productivos (solo productivas)
    query_prod = f"""
    SELECT SUM(KgTotal) AS KgProd, SUM(Cantidad) AS PzProd
    FROM Produccion
    WHERE Area IN ('{"','".join(AREAS_PRODUCTIVAS)}')
      AND ({fecha_case}) BETWEEN '{desde}' AND '{hasta}';
    """

    # Totales acabados (pintura + galvanizado)
    query_ac = f"""
    SELECT Area, SUM(KgTotal) AS KgAc, SUM(Cantidad) AS PzAc
    FROM Produccion
    WHERE Area IN ('{"','".join(AREAS_ACABADO)}')
      AND ({fecha_case}) BETWEEN '{desde}' AND '{hasta}'
    GROUP BY Area;
    """

    prod_row = ejecutar_consulta_sql(query_prod, fetchall=True) or []
    ac_rows = ejecutar_consulta_sql(query_ac, fetchall=True) or []

    kg_prod = float(prod_row[0].get("KgProd") or 0) if prod_row else 0.0
    pz_prod = int(prod_row[0].get("PzProd") or 0) if prod_row else 0

    acabado_totales = {}
    for r in ac_rows:
        area = r.get("Area")
        acabado_totales[area] = {"Kg": float(r.get("KgAc") or 0), "Piezas": int(r.get("PzAc") or 0)}

    # Porcentajes respecto a productivas
    pct_pintura = 0.0
    pct_galv = 0.0
    #buscar keys
    pz_pint = 0
    pz_galv = 0
    for area,vals in acabado_totales.items():
        if "PINTURA" in area:
            pz_pint += vals.get("Piezas",0)
        if "GALVANIZADO" in area:
            pz_galv += vals.get("Piezas",0)
    pct_pintura = (pz_pint / pz_prod * 100) if pz_prod else 0.0
    pct_galv = (pz_galv / pz_prod * 100) if pz_prod else 0.0

    return {"kg_prod": kg_prod, "pz_prod": pz_prod, "acabados": acabado_totales, "pct_pintura": pct_pintura, "pct_galv": pct_galv}

# ---------------- API: DETALLE ----------------
@router.get("/api/detalle")
def api_detalle(area: str = Query(...), desde: str = Query(...), hasta: str = Query(...), access_token: str = Cookie(None)):
    payload = validar_token(access_token)
    if not payload:
        return JSONResponse(status_code=403, content={"error": "Acceso denegado"})

    fecha_case = fecha_col_case()

    query = f"""
    SELECT
        {get_diaturno_case('Hora')} AS DiaTurno,
        {get_turno_case('Hora')} AS Turno,
        {get_bloque_2h('Hora')} AS Bloque_Horas,
        Pedido,
        Partida,
        Descripcion,
        Area,
        Cantidad,
        KgTotal,
        CASE WHEN Area IN ('ENSAMBLE','CIMSA/ ENSAMBLE','PINTURA','CIMSA/ PINTURA','GALVANIZADO') THEN Hora ELSE Fecha END AS FechaHora
    FROM Produccion
    WHERE AREA = '{area}'
      AND ({fecha_case}) BETWEEN '{desde}' AND '{hasta}'
    ORDER BY DiaTurno, Area, Turno, Bloque_Horas, FechaHora;
    """

    rows = ejecutar_consulta_sql(query, fetchall=True) or []
    return {"detalle": rows}    
