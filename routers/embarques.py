# routers/embarques.py

from fastapi import APIRouter, Request, Cookie, Query
from fastapi.responses import JSONResponse, HTMLResponse
from fastapi.templating import Jinja2Templates
from utils.auth import verificar_access_token 
from services.db_service import ejecutar_consulta_sql
from datetime import datetime, date
import calendar # Para obtener el nombre del mes

router = APIRouter()
templates = Jinja2Templates(directory="templates")

# MISMOS ACCESOS QUE EL DASHBOARD DE QUEJAS/DIRECCIÓN
EMPLEADOS_PERMITIDOS = [8811, 8661, 8870, 8740, 4, 5]

def validar_token_embarques(access_token: str):
    """Verifica el token y el K_Empleado contra la lista de permitidos."""
    if not access_token:
        return None
    token = access_token.replace("Bearer ", "")
    payload = verificar_access_token(token)
    if not payload:
        return None
    
    if payload.get("K_Empleado") in EMPLEADOS_PERMITIDOS:
        return payload
    return None


# --- CONSULTAS SQL (adaptadas para MES y AÑO dinámicos) ---

def get_query_embarques_diarios(mes: int, anio: int) -> str:
    """Query 1: Embarques realizados por día y Kgs totales (Estral y CIMSA)."""
    return f"""
    -- EMBARQUES REALIZADOS POR DIA Y KGS TOTALES (REMISIONES ÚNICAS + PEDIDOS)
    SELECT 
        CONVERT(date, e.Fecha) AS DiaEmbarque,
        'ESTRAL' AS Planta,
        COUNT(DISTINCT e.Remision) AS Total_de_Viajes,
        (SELECT STRING_AGG(CAST(Remision AS VARCHAR(20)), ', ')
          FROM (SELECT DISTINCT Remision 
                FROM Embarques e2 
                WHERE CONVERT(date, e2.Fecha) = CONVERT(date, e.Fecha)) AS R) AS Lista_Remisiones,
        COUNT(DISTINCT e.Pedido) AS Total_Pedidos,
        (SELECT STRING_AGG(CAST(Pedido AS VARCHAR(50)), ', ')
          FROM (SELECT DISTINCT Pedido 
                FROM Embarques e3 
                WHERE CONVERT(date, e3.Fecha) = CONVERT(date, e.Fecha)) AS P) AS Lista_Pedidos,
        ROUND(SUM(e.KgTotal), 2) AS Total_Kg_Dia
    FROM Embarques e
    WHERE YEAR(e.Fecha) = {anio}
      AND MONTH(e.Fecha) = {mes}
    GROUP BY CONVERT(date, e.Fecha)

    UNION ALL

    SELECT 
        CONVERT(date, c.Fecha) AS DiaEmbarque,
        'CIMSA' AS Planta,
        COUNT(DISTINCT c.Remision) AS Total_de_Viajes,
        (SELECT STRING_AGG(CAST(Remision AS VARCHAR(20)), ', ')
          FROM (SELECT DISTINCT Remision 
                FROM CimsaEmbarques c2 
                WHERE CONVERT(date, c2.Fecha) = CONVERT(date, c.Fecha)) AS R) AS Lista_Remisiones,
        COUNT(DISTINCT c.Pedido) AS Total_Pedidos,
        (SELECT STRING_AGG(CAST(Pedido AS VARCHAR(50)), ', ')
          FROM (SELECT DISTINCT Pedido 
                FROM CimsaEmbarques c3 
                WHERE CONVERT(date, c3.Fecha) = CONVERT(date, c.Fecha)) AS P) AS Lista_Pedidos,
        ROUND(SUM(c.KgTotal), 2) AS Total_Kg_Dia
    FROM CimsaEmbarques c
    WHERE YEAR(c.Fecha) = {anio}
      AND MONTH(c.Fecha) = {mes}
    GROUP BY CONVERT(date, c.Fecha)
    ORDER BY DiaEmbarque, Planta;
    """

def get_query_progreso_mensual(mes: int, anio: int) -> str:
    """Query 3: Progreso de embarque (Historico vs Mes) basado en WS_Planeacion."""
    return f"""
    -- CON BASE AL MES PROGRAMADO, TENEMOS EL EMBARQUE HISTORICO DEL PEDIDO Y EN EL MES CUANTOS HAN SIDO
    SELECT 
        t.Pedido,
        t.Embarques_Historico,
        t.Embarques_Mes,
        ROUND(t.Kg_Historico, 2) AS Kg_Historico,
        ROUND(t.Kg_Mes, 2) AS Kg_Mes
    FROM (
        SELECT 
            p.Pedido,
            COUNT(DISTINCT e.Remision) AS Embarques_Historico,
            COUNT(DISTINCT CASE 
                                WHEN YEAR(e.Fecha) = {anio}
                                 AND MONTH(e.Fecha) = {mes}
                                THEN e.Remision 
                            END) AS Embarques_Mes,
            ISNULL(SUM(e.KgTotal), 0) AS Kg_Historico,
            ISNULL(SUM(CASE 
                                 WHEN YEAR(e.Fecha) = {anio}
                                 AND MONTH(e.Fecha) = {mes}
                                 THEN e.KgTotal 
                             END), 0) AS Kg_Mes
        FROM WS_Planeacion p
        LEFT JOIN (
            SELECT Pedido, Remision, Fecha, KgTotal FROM Embarques
            UNION ALL
            SELECT Pedido, Remision, Fecha, KgTotal FROM CimsaEmbarques
        ) e
            ON p.Pedido = e.Pedido
        GROUP BY p.Pedido
    ) t

    UNION ALL

    -- 🟩 total general
    SELECT
        'TOTAL MES' AS Pedido,
        SUM(t.Embarques_Historico) AS Embarques_Historico,
        SUM(t.Embarques_Mes) AS Embarques_Mes,
        ROUND(SUM(t.Kg_Historico), 2) AS Kg_Historico,
        ROUND(SUM(t.Kg_Mes), 2) AS Kg_Mes
    FROM (
        SELECT 
            p.Pedido,
            COUNT(DISTINCT e.Remision) AS Embarques_Historico,
            COUNT(DISTINCT CASE 
                                WHEN YEAR(e.Fecha) = {anio}
                                 AND MONTH(e.Fecha) = {mes}
                                THEN e.Remision 
                            END) AS Embarques_Mes,
            ISNULL(SUM(e.KgTotal), 0) AS Kg_Historico,
            ISNULL(SUM(CASE 
                                 WHEN YEAR(e.Fecha) = {anio}
                                 AND MONTH(e.Fecha) = {mes}
                                 THEN e.KgTotal 
                             END), 0) AS Kg_Mes
        FROM WS_Planeacion p
        LEFT JOIN (
            SELECT Pedido, Remision, Fecha, KgTotal FROM Embarques
            UNION ALL
            SELECT Pedido, Remision, Fecha, KgTotal FROM CimsaEmbarques
        ) e
            ON p.Pedido = e.Pedido
        GROUP BY p.Pedido
    ) t;
    """

# CONSULTA SQL PARA LA TENDENCIA ANUAL
def get_query_tendencia_anual(anio: int) -> str:
    """Query 4: Kilos totales embarcados por mes para el año seleccionado."""
    # Como ya tienes Embarques y CimsaEmbarques unidos en otras consultas,
    # lo haré aquí directamente para sumarizar por mes.
    return f"""
    SELECT
        MONTH(e.Fecha) AS Mes,
        ROUND(SUM(e.KgTotal), 2) AS Total_Kg
    FROM (
        -- UNION de Embarques Estral y Cimsa para tener todos los datos
        SELECT Fecha, KgTotal FROM Embarques
        UNION ALL
        SELECT Fecha, KgTotal FROM CimsaEmbarques
    ) e
    WHERE YEAR(e.Fecha) = {anio}
    GROUP BY MONTH(e.Fecha)
    ORDER BY Mes ASC;
    """

# ----------------------------------------------------

# --- RUTA PRINCIPAL ---

@router.get("/embarques", response_class=HTMLResponse)
def embarques_page(
    request: Request,
    access_token: str = Cookie(None),
    mes: int = Query(None),
    anio: int = Query(None)
):
    # 1. Validación de Acceso
    payload = validar_token_embarques(access_token)
    if not payload:
        return JSONResponse(status_code=403, content={"error": "Acceso denegado. No tienes permisos para esta sección."})

    # 2. Manejo de Parámetros de Tiempo (Mes y Año)
    today = datetime.now()
    if mes is None:
        mes = today.month
    if anio is None:
        anio = today.year

    try:
        # 3. Ejecución de Consultas
        sql_diarios = get_query_embarques_diarios(mes, anio)
        data_diarios = ejecutar_consulta_sql(sql_diarios, fetchall=True)

        sql_progreso = get_query_progreso_mensual(mes, anio)
        data_progreso = ejecutar_consulta_sql(sql_progreso, fetchall=True)
        
        # Esto evita el error "TypeError: Object of type date is not JSON serializable" 
        # cuando se usa el filtro 'tojson' en Jinja.
        for item in data_diarios:
            if 'DiaEmbarque' in item and isinstance(item['DiaEmbarque'], date):
                # Usar strftime para convertir el objeto date a string 'YYYY-MM-DD'
                item['DiaEmbarque'] = item['DiaEmbarque'].strftime('%Y-%m-%d')

    except Exception as e:
        # Si la consulta a la BD falla, devolvemos el error 500
        return JSONResponse(status_code=500, content={"error": "Error al consultar la base de datos de Embarques.", "detail": str(e)})

    # 4. Preparación de Datos
    # calendar.month_name es en inglés, por eso se puede usar el mapping si se requiere español, pero lo dejaremos así por ahora si ya está funcionando.
    nombre_mes = calendar.month_name[mes].capitalize()
    
    # Separar Total (KPI) del Detalle
    progreso_total = next((item for item in data_progreso if item['Pedido'] == 'TOTAL MES'), {})
    progreso_detalle = [item for item in data_progreso if item['Pedido'] != 'TOTAL MES']


    # 5. Renderización de la Template
    return templates.TemplateResponse("embarques.html", {
        "request": request,
        "usuario": payload.get("sub", "Usuario"),
        "mes_actual": mes,
        "anio_actual": anio,
        "nombre_mes": nombre_mes,
        "anio": anio,
        # data_diarios ya tiene las fechas como strings
        "data_diarios": data_diarios, 
        "progreso_total": progreso_total,
        "progreso_detalle": progreso_detalle
    })

# ENDPOINT PARA LA GRÁFICA MENSUAL (CONSUMIDO POR JAVASCRIPT)
@router.get("/embarques/tendencia_anual", response_class=JSONResponse)
def tendencia_anual_endpoint(
    access_token: str = Cookie(None),
    anio: int = Query(None)
):
    # 1. Validación de Acceso
    payload = validar_token_embarques(access_token)
    if not payload:
        return JSONResponse(status_code=403, content={"error": "Acceso denegado."})

    # 2. Manejo de Parámetros de Tiempo
    if anio is None:
        return JSONResponse(status_code=400, content={"error": "El parámetro 'anio' es requerido."})

    try:
        # 3. Ejecución de la Consulta SQL
        sql_tendencia = get_query_tendencia_anual(anio)
        
        # Ejecutar la consulta y obtener los resultados
        # Nota: Asumo que ejecutar_consulta_sql devuelve una lista de diccionarios/objetos
        resultados = ejecutar_consulta_sql(sql_tendencia, fetchall=True)
        
    except Exception as e:
        # 4. Manejo de errores de BD
        return JSONResponse(status_code=500, content={"error": "Error al consultar la tendencia anual.", "detail": str(e)})

    # 5. Devolver el JSON con el formato que espera el frontend
    return JSONResponse(content={
        "tendencia_mensual": resultados
    })