# routers/fabricacion_mensual_partidas.py
from fastapi import APIRouter, Request, Query, Cookie
from fastapi.responses import JSONResponse, HTMLResponse
from fastapi.templating import Jinja2Templates
from utils.auth import verificar_access_token
from services.db_service import ejecutar_consulta_sql
from datetime import datetime, timedelta, date # Agregamos 'date' para el tipado
from typing import Dict, Any

router = APIRouter()
templates = Jinja2Templates(directory="templates")

# ---------------- CONFIG ----------------
AREAS_PERMITIDAS = [20, 22]
EMPLEADOS_PERMITIDOS = [8811, 8661, 8870, 8740, 4, 5]

AREAS_PRODUCTIVAS = ("HABILITADO","PERFILADO","ENSAMBLE","CIMSA/ ENSAMBLE")
AREAS_ACABADO = ("PINTURA","CIMSA/ PINTURA","GALVANIZADO")

# ---------------- HELPERS ----------------
def validar_token(access_token: str):
    """ Valida el token y verifica permisos de área o empleado. """
    if not access_token:
        return None
    token = access_token.replace("Bearer ", "")
    payload = verificar_access_token(token)
    if not payload:
        return None
    if (payload.get("K_Area") in AREAS_PERMITIDAS) or (payload.get("K_Empleado") in EMPLEADOS_PERMITIDOS):
        return payload
    return None

def _get_default_dates(desde: str | None, hasta: str | None) -> tuple[str, str]:
    """ Calcula o formatea las fechas 'desde' y 'hasta' con valores por defecto. """
    today = datetime.today()
    
    if not hasta:
        # Usa el día de mañana (o el siguiente) como límite superior exclusivo
        hasta_dt = today + timedelta(days=1)
        hasta = hasta_dt.strftime("%Y-%m-%d")
    else:
        # Asume formato YYYY-MM-DD
        hasta_dt = datetime.strptime(hasta, "%Y-%m-%d")

    if not desde:
        # Por defecto, el primer día del mes de la fecha 'hasta'
        desde_dt = hasta_dt.replace(day=1)
        desde = desde_dt.strftime("%Y-%m-%d")

    return desde, hasta

def fecha_col_case() -> str:
    """ Retorna el CASE de SQL para seleccionar entre 'Hora' o 'Fecha' según el área. """
    return "CASE WHEN Area IN ('ENSAMBLE','CIMSA/ ENSAMBLE','PINTURA','CIMSA/ PINTURA','GALVANIZADO') THEN Hora ELSE Fecha END"

def get_diaturno_case(col_fecha: str) -> str:
    """ Retorna el CASE de SQL para calcular el 'DíaTurno' (fecha contable). """
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
    """ Retorna el CASE de SQL para calcular el 'Turno' ('DIA' o 'NOCHE'). """
    return f"""
    CASE 
        WHEN Area IN ('ENSAMBLE','CIMSA/ ENSAMBLE','PINTURA','CIMSA/ PINTURA') 
             THEN CASE WHEN DATEPART(HOUR, {col_fecha}) BETWEEN 7 AND 18 THEN 'DIA' ELSE 'NOCHE' END
        ELSE CASE WHEN DATEPART(HOUR, {col_fecha}) BETWEEN 7 AND 18 THEN 'DIA' ELSE 'NOCHE' END
    END
    """

def get_bloque_2h(col_fecha: str) -> str:
    """ Retorna el CASE de SQL para calcular el 'Bloque' de 2 horas. """
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
    
# Nueva función interna (sin decorador @router.get) que contiene la lógica de consulta
def _get_resumen_data(desde: str, hasta: str) -> Dict[str, Any]:
    """ Lógica principal de consulta para el resumen agrupado por día/turno/bloque. """
    fecha_case = fecha_col_case()

    prod_areas_sql = "','".join(AREAS_PRODUCTIVAS + AREAS_ACABADO)

    query = f"""
    WITH cte AS (
        SELECT
            Area,
            KgTotal,
            Cantidad,
            {get_diaturno_case(fecha_case)} AS DiaTurno,
            {get_turno_case(fecha_case)} AS Turno,
            {get_bloque_2h(fecha_case)} AS Bloque
        FROM Produccion
        WHERE AREA IN ('{prod_areas_sql}')
          AND ({fecha_case}) BETWEEN '{desde}' AND '{hasta}'
    )
    SELECT DiaTurno, Turno, Bloque, Area,
            SUM(KgTotal) AS PesoKg,
            SUM(Cantidad) AS Piezas
    FROM cte
    GROUP BY DiaTurno, Turno, Bloque, Area
    ORDER BY DiaTurno, Area, Turno, Bloque;
    """

    rows = ejecutar_consulta_sql(query, fetchall=True) or []

    total_kg, total_pzas = 0.0, 0
    resumen = []

    for r in rows:
        peso = float(r.get("PesoKg") or 0)
        pzas = int(r.get("Piezas") or 0)
        area = r.get("Area")

        # Solo suma las áreas productivas, no acabados
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


# Nueva función interna (sin decorador @router.get) que implementa la lógica simple de total
def _get_totales_simples_mes_actual(desde: str, hasta: str) -> Dict[str, Any]:
    """ 
    Calcula totales de Kg y Piezas de Áreas Productivas usando SOLO el filtro de Mes/Año de la columna Fecha.
    Esta lógica replica el query que confirmaste como correcto para el total contable.
    """
    prod_areas_sql = "','".join(AREAS_PRODUCTIVAS)
    
    # Extraemos Mes y Año de la fecha 'desde' proporcionada (que generalmente es el primer día del mes)
    try:
        desde_dt = datetime.strptime(desde, "%Y-%m-%d")
        mes = desde_dt.month
        anio = desde_dt.year
    except ValueError:
        # Fallback de seguridad, aunque _get_default_dates debería prevenir esto
        today = datetime.today()
        mes = today.month
        anio = today.year

    query = f"""
    SELECT 
        SUM(KgTotal) AS total_kg, 
        SUM(Cantidad) AS total_piezas
    FROM 
        Produccion
    WHERE 
        Area IN ('{prod_areas_sql}')
        -- Usamos la lógica simple de Mes y Año en la columna Fecha para máxima precisión.
        AND YEAR(Fecha) = {anio} 
        AND MONTH(Fecha) = {mes}; 
    """

    row = ejecutar_consulta_sql(query, fetchone=True)
    
    total_kg = float(row.get("total_kg") or 0.0) if row else 0.0
    total_piezas = int(row.get("total_piezas") or 0) if row else 0
    
    return {"total_kg": total_kg, "total_piezas": total_piezas}

# ---------------- PAGE ----------------
@router.get("/", response_class=HTMLResponse)
def partidas_page(request: Request, access_token: str = Cookie(None)):
    """ Renderiza la página principal con fechas por defecto. """
    payload = validar_token(access_token)
    if not payload:
        return JSONResponse(status_code=403, content={"error": "Acceso denegado"})
        
    # La lógica de fechas iniciales de la página
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
    """ Retorna el resumen agrupado por DiaTurno, Turno y Bloque por área. """
    payload = validar_token(access_token)
    if not payload:
        return JSONResponse(status_code=403, content={"error": "Acceso denegado"})

    # Usamos el helper para establecer las fechas
    desde, hasta = _get_default_dates(desde, hasta)

    return _get_resumen_data(desde, hasta)



@router.get("/api/tendencia_turnos")
def api_tendencia_turnos(desde: str = Query(None), hasta: str = Query(None), access_token: str = Cookie(None)):
    """ Retorna la tendencia de producción agrupada por DíaTurno y Turno,
        sin duplicar registros de áreas de acabado. """
    payload = validar_token(access_token)
    if not payload:
        return JSONResponse(status_code=403, content={"error": "Acceso denegado"})
        
    desde, hasta = _get_default_dates(desde, hasta)
    fecha_case = fecha_col_case()

    prod_areas_sql = "','".join(AREAS_PRODUCTIVAS)
    acabado_areas_sql = "','".join(AREAS_ACABADO)

    # Extrae Mes y Año de la fecha de inicio
    try:
        desde_dt = datetime.strptime(desde, "%Y-%m-%d")
        mes = desde_dt.month
        anio = desde_dt.year
    except ValueError:
        today = datetime.today()
        mes = today.month
        anio = today.year

    # 🔥 CORREGIDO: Agrupamos CTEs antes del JOIN para evitar duplicados
    query = f"""
    WITH cte_prod AS (
        SELECT
            {get_diaturno_case(fecha_case)} AS DiaTurno,
            {get_turno_case(fecha_case)} AS Turno,
            SUM(KgTotal) AS KgTotal,
            SUM(Cantidad) AS Cantidad
        FROM Produccion
        WHERE Area IN ('{prod_areas_sql}')
          AND YEAR(Fecha) = {anio}
          AND MONTH(Fecha) = {mes}
          AND Fecha BETWEEN '{desde}' AND '{hasta}'
        GROUP BY {get_diaturno_case(fecha_case)}, {get_turno_case(fecha_case)}
    ),
    cte_acabado AS (
        SELECT
            {get_diaturno_case(fecha_case)} AS DiaTurno,
            {get_turno_case(fecha_case)} AS Turno,
            SUM(KgTotal) AS KgTotal,
            SUM(Cantidad) AS Cantidad
        FROM Produccion
        WHERE Area IN ('{acabado_areas_sql}')
          AND YEAR(Fecha) = {anio}
          AND MONTH(Fecha) = {mes}
          AND Fecha BETWEEN '{desde}' AND '{hasta}'
        GROUP BY {get_diaturno_case(fecha_case)}, {get_turno_case(fecha_case)}
    )
    SELECT
        p.DiaTurno,
        p.Turno,
        p.KgTotal AS Kg_Productivo,
        p.Cantidad AS Pz_Productivo,
        ISNULL(a.KgTotal, 0) AS Kg_Acabado,
        ISNULL(a.Cantidad, 0) AS Pz_Acabado
    FROM cte_prod p
    LEFT JOIN cte_acabado a
        ON a.DiaTurno = p.DiaTurno AND a.Turno = p.Turno
    ORDER BY p.DiaTurno, p.Turno;
    """

    rows = ejecutar_consulta_sql(query, fetchall=True) or []

    suma_por_dia = {}
    dias_orden = []

    # Procesamos los resultados
    for r in rows:
        dia = r.get("DiaTurno")
        if hasattr(dia, "strftime"):
            dia = dia.strftime("%Y-%m-%d")
        turno = r.get("Turno")

        if dia not in suma_por_dia:
            suma_por_dia[dia] = {
                "DIA": {"kg": 0, "pz": 0},
                "NOCHE": {"kg": 0, "pz": 0},
                "DIA_AC": {"kg": 0, "pz": 0},
                "NOCHE_AC": {"kg": 0, "pz": 0}
            }
            dias_orden.append(dia)

        # Producción productiva
        suma_por_dia[dia][turno]["kg"] = float(r.get("Kg_Productivo") or 0)
        suma_por_dia[dia][turno]["pz"] = int(r.get("Pz_Productivo") or 0)

        # Acabado (pintura/galvanizado)
        turno_acabado = turno.replace("DIA", "DIA_AC").replace("NOCHE", "NOCHE_AC")
        suma_por_dia[dia][turno_acabado]["kg"] = float(r.get("Kg_Acabado") or 0)
        suma_por_dia[dia][turno_acabado]["pz"] = int(r.get("Pz_Acabado") or 0)

    dias_orden = sorted(list(set(dias_orden)))

    # Series para el gráfico
    dia_kg = [suma_por_dia[d]["DIA"]["kg"] for d in dias_orden]
    noche_kg = [suma_por_dia[d]["NOCHE"]["kg"] for d in dias_orden]
    dia_ac_kg = [suma_por_dia[d]["DIA_AC"]["kg"] for d in dias_orden]
    noche_ac_kg = [suma_por_dia[d]["NOCHE_AC"]["kg"] for d in dias_orden]

    # Totales
    total_dia_kg = sum(dia_kg)
    total_noche_kg = sum(noche_kg)

    return {
        "dias": dias_orden,
        "dia_kg": dia_kg, "noche_kg": noche_kg,
        "total_dia_kg": total_dia_kg,
        "total_noche_kg": total_noche_kg,
        "dia_pz": [suma_por_dia[d]["DIA"]["pz"] for d in dias_orden], 
        "noche_pz": [suma_por_dia[d]["NOCHE"]["pz"] for d in dias_orden],
        "dia_ac_kg": dia_ac_kg, "noche_ac_kg": noche_ac_kg,
        "dia_ac_pz": [suma_por_dia[d]["DIA_AC"]["pz"] for d in dias_orden], 
        "noche_ac_pz": [suma_por_dia[d]["NOCHE_AC"]["pz"] for d in dias_orden]
    }


# ---------------- API: TENDENCIA POR BLOQUES (CORREGIDA) ----------------
@router.get("/api/tendencia_bloques")
def api_tendencia_bloques(desde: str = Query(None), hasta: str = Query(None), access_token: str = Cookie(None)):
    """ Retorna la tendencia de produccion agrupada por Bloque de 2h y Turno (sin duplicar areas de acabado). """
    payload = validar_token(access_token)
    if not payload:
        return JSONResponse(status_code=403, content={"error": "Acceso denegado"})
        
    desde, hasta = _get_default_dates(desde, hasta)
    fecha_case = fecha_col_case()

    prod_areas_sql = "','".join(AREAS_PRODUCTIVAS)
    acabado_areas_sql = "','".join(AREAS_ACABADO) 

    # Logica para obtener Mes y Año
    try:
        desde_dt = datetime.strptime(desde, "%Y-%m-%d")
        mes = desde_dt.month
        anio = desde_dt.year
    except ValueError:
        today = datetime.today()
        mes = today.month
        anio = today.year

    query = f"""
    WITH cte_prod AS (
        SELECT KgTotal, Cantidad,
                {get_bloque_2h(fecha_case)} AS Bloque,
                {get_turno_case(fecha_case)} AS Turno
        FROM Produccion
        WHERE Area IN ('{prod_areas_sql}')
          -- Filtro Contable: Mes y Año
          AND YEAR(Fecha) = {anio} 
          AND MONTH(Fecha) = {mes}
          -- Filtro de Rango (CRITICO)
          AND Fecha BETWEEN '{desde}' AND '{hasta}'
    ),
    cte_acabado AS (
        SELECT KgTotal, Cantidad,
                {get_bloque_2h(fecha_case)} AS Bloque,
                {get_turno_case(fecha_case)} AS Turno
        FROM Produccion
        WHERE Area IN ('{acabado_areas_sql}')
          -- Filtro Contable: Mes y Año
          AND YEAR(Fecha) = {anio} 
          AND MONTH(Fecha) = {mes}
          -- Filtro de Rango (CRITICO)
          AND Fecha BETWEEN '{desde}' AND '{hasta}'
    )
    SELECT
        p.Bloque,
        p.Turno,
        SUM(p.KgTotal) AS Kg_Productivo,
        SUM(p.Cantidad) AS Pz_Productivo,
        ISNULL(SUM(a.KgTotal),0) AS Kg_Acabado,
        ISNULL(SUM(a.Cantidad),0) AS Pz_Acabado
    FROM cte_prod p
    LEFT JOIN cte_acabado a
        ON a.Bloque = p.Bloque AND a.Turno = p.Turno
    GROUP BY p.Bloque, p.Turno
    ORDER BY p.Bloque;
    """
    
    rows = ejecutar_consulta_sql(query, fetchall=True) or []

    bloques_orden = sorted(list({r["Bloque"] for r in rows if r.get("Bloque")}))
    prod_day, prod_night, ac_day, ac_night = [], [], [], []

    for b in bloques_orden:
        prod_day.append(sum(float(r.get("Kg_Productivo") or 0) for r in rows if r.get("Bloque") == b and r.get("Turno") == "DIA"))
        prod_night.append(sum(float(r.get("Kg_Productivo") or 0) for r in rows if r.get("Bloque") == b and r.get("Turno") == "NOCHE"))
        ac_day.append(sum(float(r.get("Kg_Acabado") or 0) for r in rows if r.get("Bloque") == b and r.get("Turno") == "DIA"))
        ac_night.append(sum(float(r.get("Kg_Acabado") or 0) for r in rows if r.get("Bloque") == b and r.get("Turno") == "NOCHE"))

    return {"bloques": bloques_orden, "prod_day": prod_day, "prod_night": prod_night, "ac_day": ac_day, "ac_night": ac_night}

# ---------------- API: ACABADOS ----------------
@router.get("/api/acabados")
def api_acabados(desde: str = Query(None), hasta: str = Query(None), access_token: str = Cookie(None)):
    """ Retorna los totales de producción vs. acabados para calcular eficiencias (usando filtro mensual simple). """
    payload = validar_token(access_token)
    if not payload:
        return JSONResponse(status_code=403, content={"error": "Acceso denegado"})
        
    desde, hasta = _get_default_dates(desde, hasta)
    
    prod_areas_sql = "','".join(AREAS_PRODUCTIVAS)
    acabado_areas_sql = "','".join(AREAS_ACABADO)

    # Lógica simple: Extraer Mes y Año de la fecha 'desde' (que es el primer día del mes por defecto)
    try:
        desde_dt = datetime.strptime(desde, "%Y-%m-%d")
        mes = desde_dt.month
        anio = desde_dt.year
    except ValueError:
        today = datetime.today()
        mes = today.month
        anio = today.year

    # Query de Producción (Usa el filtro simple de Mes/Año en la columna Fecha)
    query_prod = f"""
    SELECT SUM(KgTotal) AS KgProd, SUM(Cantidad) AS PzProd
    FROM Produccion
    WHERE Area IN ('{prod_areas_sql}')
      AND YEAR(Fecha) = {anio} 
      AND MONTH(Fecha) = {mes};
    """
    # Query de Acabados (Usa el filtro simple de Mes/Año en la columna Fecha)
    query_ac = f"""
    SELECT Area, SUM(KgTotal) AS KgAc, SUM(Cantidad) AS PzAc
    FROM Produccion
    WHERE Area IN ('{acabado_areas_sql}')
      AND YEAR(Fecha) = {anio} 
      AND MONTH(Fecha) = {mes}
    GROUP BY Area;
    """

    prod_row = ejecutar_consulta_sql(query_prod, fetchall=True) or []
    ac_rows = ejecutar_consulta_sql(query_ac, fetchall=True) or []

    kg_prod = float(prod_row[0].get("KgProd") or 0) if prod_row and prod_row[0] and prod_row[0].get("KgProd") is not None else 0.0
    pz_prod = int(prod_row[0].get("PzProd") or 0) if prod_row and prod_row[0] and prod_row[0].get("PzProd") is not None else 0

    acabado_totales = {}
    for r in ac_rows:
        area = r.get("Area")
        acabado_totales[area] = {"Kg": float(r.get("KgAc") or 0), "Piezas": int(r.get("PzAc") or 0)}

    # Calculo de porcentajes (usando las áreas definidas)
    # SUMA de piezas en áreas de pintura (PINTURA, CIMSA/PINTURA)
    pz_pintura = sum(v["Piezas"] for k,v in acabado_totales.items() if any(a in k for a in ("PINTURA", "CIMSA/ PINTURA")))
    pct_pintura = (pz_pintura / pz_prod * 100) if pz_prod else 0.0

    # SUMA de piezas en áreas de galvanizado (GALVANIZADO)
    pz_galv = sum(v["Piezas"] for k,v in acabado_totales.items() if k == "GALVANIZADO")
    pct_galv = (pz_galv / pz_prod * 100) if pz_prod else 0.0


    return {
        "kg_prod": kg_prod, 
        "pz_prod": pz_prod, 
        "acabados": acabado_totales, 
        "pct_pintura": pct_pintura, 
        "pct_galv": pct_galv
    }

# ---------------- API: DETALLE ----------------
@router.get("/api/detalle")
def api_detalle(desde: str = Query(None), hasta: str = Query(None), area: str = Query(None), access_token: str = Cookie(None)):
    """ Retorna el detalle de producción sin agrupar, filtrado opcionalmente por área. """
    payload = validar_token(access_token)
    if not payload:
        return JSONResponse(status_code=403, content={"error": "Acceso denegado"})
        
    desde, hasta = _get_default_dates(desde, hasta)
    fecha_case = fecha_col_case()
    
    # Previene inyección SQL básica si 'area' no está en la lista permitida, aunque ya se valida token
    areas_validas = list(AREAS_PRODUCTIVAS) + list(AREAS_ACABADO)
    area_filter = f"AND Area='{area}'" if area and area in areas_validas else ""

    # Se agregan columnas adicionales del query original (Pedido, Partida, Descripcion, etc.)
    # Asumo que Produccion tiene esas columnas.
    query = f"""
    SELECT Pedido, Partida, Descripcion, Area, Cantidad, KgTotal, 
           {get_diaturno_case(fecha_case)} AS DiaTurno, 
           {get_turno_case(fecha_case)} AS Turno, 
           {get_bloque_2h(fecha_case)} AS Bloque,
           {fecha_case} AS FechaHora
    FROM Produccion
    WHERE ({fecha_case}) BETWEEN '{desde}' AND '{hasta}' {area_filter}
    ORDER BY DiaTurno, Area, Turno, Bloque, FechaHora;
    """

    rows = ejecutar_consulta_sql(query, fetchall=True) or []
    detalle = []
    for r in rows:
        dia = r.get("DiaTurno")
        if hasattr(dia, "strftime"):
            dia = dia.strftime("%Y-%m-%d")
            
        fechahora = r.get("FechaHora")
        if hasattr(fechahora, "strftime"):
            fechahora = fechahora.strftime("%Y-%m-%d %H:%M:%S")

        detalle.append({
            "DiaTurno": dia,
            "Turno": r.get("Turno"),
            "Bloque": r.get("Bloque"),
            "Pedido": r.get("Pedido"),
            "Partida": r.get("Partida"),
            "Descripcion": r.get("Descripcion"),
            "Area": r.get("Area"),
            "PesoKg": float(r.get("KgTotal") or 0),
            "Piezas": int(r.get("Cantidad") or 0),
            "FechaHora": fechahora
        })

    return {"detalle": detalle}

# ---------------- API: TOTALES PRODUCCION ----------------
# Reemplazar el bloque original:
# ---------------- API: TOTALES PRODUCCION ----------------
@router.get("/api/totales_produccion")
def api_totales_produccion(desde: str = Query(None), hasta: str = Query(None), access_token: str = Cookie(None)):
    """ Retorna los totales de peso (Kg) y piezas de las áreas productivas. """
    payload = validar_token(access_token)
    if not payload:
        return JSONResponse(status_code=403, content={"error": "Acceso denegado"})

    desde, hasta = _get_default_dates(desde, hasta)

    # REEMPLAZO: Usa la nueva lógica simple que replica tu query correcto
    data = _get_totales_simples_mes_actual(desde, hasta) 
    
    return data # Retorna solo {"total_kg": X, "total_piezas": Y}