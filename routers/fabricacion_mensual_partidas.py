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
        WHEN Area IN ('ENSAMBLE','CIMSA/ ENSAMBLE','PINTURA','CIMSA/ PINTURA') 
            THEN CASE WHEN DATEPART(HOUR, {col_fecha}) >= 7 THEN CONVERT(date, {col_fecha})
                      ELSE DATEADD(DAY, -1, CONVERT(date, {col_fecha})) END
        ELSE CASE WHEN DATEPART(HOUR, {col_fecha}) >= 7 THEN CONVERT(date, {col_fecha})
                  ELSE DATEADD(DAY, -1, CONVERT(date, {col_fecha})) END
    END
    """

def get_turno_case(col_fecha: str) -> str:
    return f"""
    CASE 
        WHEN Area IN ('ENSAMBLE','CIMSA/ ENSAMBLE','PINTURA','CIMSA/ PINTURA') 
            THEN CASE WHEN DATEPART(HOUR, {col_fecha}) BETWEEN 7 AND 18 THEN 'Día' ELSE 'Noche' END
        ELSE CASE WHEN DATEPART(HOUR, {col_fecha}) BETWEEN 7 AND 18 THEN 'Día' ELSE 'Noche' END
    END
    """

def get_bloque_2h(col_fecha: str) -> str:
    return f"""
    CASE 
        WHEN Area IN ('ENSAMBLE','CIMSA/ ENSAMBLE','PINTURA','CIMSA/ PINTURA') THEN
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

def get_fecha_col(area: str) -> str:
    return "Hora" if area in ('ENSAMBLE','CIMSA/ ENSAMBLE','PINTURA','CIMSA/ PINTURA') else "Fecha"

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

# ---------------- API: RESUMEN ----------------
@router.get("/api/resumen")
def api_resumen(desde: str = Query(None), hasta: str = Query(None), access_token: str = Cookie(None)):
    payload = validar_token(access_token)
    if not payload:
        return JSONResponse(status_code=403, content={"error": "Acceso denegado"})

    if not hasta:
        hasta = (datetime.today() + timedelta(days=1)).strftime("%Y-%m-%d")
    if not desde:
        dt = datetime.strptime(hasta, "%Y-%m-%d")
        desde = dt.replace(day=1).strftime("%Y-%m-%d")

    fecha_col = "Hora"  # Para SQL CASE, usamos siempre Hora para áreas principales

    query = f"""
    SELECT
        {get_diaturno_case('Hora')} AS DiaTurno,
        {get_turno_case('Hora')} AS Turno,
        {get_bloque_2h('Hora')} AS Bloque,
        Area,
        SUM(KgTotal) AS PesoKg,
        SUM(Cantidad) AS Piezas
    FROM Produccion
    WHERE AREA IN ('ENSAMBLE','CIMSA/ ENSAMBLE','HABILITADO','PERFILADO','PINTURA','CIMSA/ PINTURA')
      AND (CASE WHEN Area IN ('ENSAMBLE','CIMSA/ ENSAMBLE','PINTURA','CIMSA/ PINTURA') THEN Hora ELSE Fecha END)
          BETWEEN '{desde}' AND '{hasta}'
    GROUP BY {get_diaturno_case('Hora')},{get_turno_case('Hora')},{get_bloque_2h('Hora')},Area
    ORDER BY DiaTurno, Area, Turno;
    """

    rows = ejecutar_consulta_sql(query, fetchall=True) or []
    total_kg = sum(float(r["PesoKg"]) for r in rows)
    total_pzas = sum(int(r["Piezas"]) for r in rows)

    resumen = [{
        "DiaTurno": r["DiaTurno"].strftime("%Y-%m-%d") if hasattr(r["DiaTurno"], "strftime") else r["DiaTurno"],
        "Turno": r["Turno"],
        "Bloque": r["Bloque"],
        "Area": r["Area"],
        "PesoKg": float(r["PesoKg"] or 0),
        "Piezas": int(r["Piezas"] or 0)
    } for r in rows]

    return {"kpis": {"total_kg": total_kg, "total_piezas": total_pzas}, "resumen": resumen}

# ---------------- API: DETALLE ----------------
@router.get("/api/detalle")
def api_detalle(area: str = Query(...), desde: str = Query(...), hasta: str = Query(...), access_token: str = Cookie(None)):
    payload = validar_token(access_token)
    if not payload:
        return JSONResponse(status_code=403, content={"error": "Acceso denegado"})

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
        CASE WHEN Area IN ('ENSAMBLE','CIMSA/ ENSAMBLE','PINTURA','CIMSA/ PINTURA') THEN Hora ELSE Fecha END AS FechaHora
    FROM Produccion
    WHERE AREA = '{area}'
      AND (CASE WHEN Area IN ('ENSAMBLE','CIMSA/ ENSAMBLE','PINTURA','CIMSA/ PINTURA') THEN Hora ELSE Fecha END)
          BETWEEN '{desde}' AND '{hasta}'
    ORDER BY DiaTurno, Area, Turno, Bloque_Horas, FechaHora;
    """

    rows = ejecutar_consulta_sql(query, fetchall=True) or []
    return {"detalle": rows}

# ---------------- API: BLOQUES ----------------
@router.get("/api/bloques")
def api_bloques(desde: str = Query(...), hasta: str = Query(...), access_token: str = Cookie(None)):
    """
    Devuelve los datos agrupados por Bloque y Área, para graficar fácilmente.
    """
    payload = validar_token(access_token)
    if not payload:
        return JSONResponse(status_code=403, content={"error": "Acceso denegado"})

    if not hasta:
        hasta = (datetime.today() + timedelta(days=1)).strftime("%Y-%m-%d")
    if not desde:
        dt = datetime.strptime(hasta, "%Y-%m-%d")
        desde = dt.replace(day=1).strftime("%Y-%m-%d")

    query = f"""
    SELECT
        {get_bloque_2h('Hora')} AS Bloque,
        Area,
        SUM(KgTotal) AS PesoKg,
        SUM(Cantidad) AS Piezas
    FROM Produccion
    WHERE AREA IN ('ENSAMBLE','CIMSA/ ENSAMBLE','HABILITADO','PERFILADO','PINTURA','CIMSA/ PINTURA')
      AND (CASE WHEN Area IN ('ENSAMBLE','CIMSA/ ENSAMBLE','PINTURA','CIMSA/ PINTURA') THEN Hora ELSE Fecha END)
          BETWEEN '{desde}' AND '{hasta}'
    GROUP BY {get_bloque_2h('Hora')}, Area
    ORDER BY Bloque, Area;
    """

    rows = ejecutar_consulta_sql(query, fetchall=True) or []

    bloques = {}
    for r in rows:
        bloque = r["Bloque"]
        if bloque not in bloques:
            bloques[bloque] = []
        bloques[bloque].append({
            "Area": r["Area"],
            "PesoKg": float(r["PesoKg"] or 0),
            "Piezas": int(r["Piezas"] or 0)
        })

    # Formato: {"Bloque1": [{"Area":..., "PesoKg":..., "Piezas":...}, ...], ...}
    return {"bloques": bloques}
