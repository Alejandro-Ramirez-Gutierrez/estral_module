from fastapi import APIRouter, Request, Cookie, Query
from fastapi.responses import JSONResponse, HTMLResponse
from fastapi.templating import Jinja2Templates
from utils.auth import verificar_access_token 
from services.db_service import ejecutar_consulta_sql # ¡ASUMO que esta función puede recibir parámetros!
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



def get_query_embarques_diarios(mes: int, anio: int) -> tuple[str, tuple]:
    """Query 1: Embarques realizados por día y Kgs totales (Estral y CIMSA).
    
    Retorna: (sql_query, parametros)
    """
    # Usaremos '?' o '%s' como placeholders. Asumo '?' ya que es común con pyodbc/ODBC.
    sql_query = """
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
    WHERE YEAR(e.Fecha) = ? -- Placeholder 1: anio
      AND MONTH(e.Fecha) = ? -- Placeholder 2: mes
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
    WHERE YEAR(c.Fecha) = ? -- Placeholder 3: anio
      AND MONTH(c.Fecha) = ? -- Placeholder 4: mes
    GROUP BY CONVERT(date, c.Fecha)
    ORDER BY DiaEmbarque, Planta;
    """
    # Los parámetros se pasan en el orden de los placeholders '?'
    parametros = (anio, mes, anio, mes)
    return sql_query, parametros

def get_query_progreso_mensual(mes: int, anio: int) -> tuple[str, tuple]:
    """Query 3: Progreso de embarque (Historico vs Mes) basado en WS_Planeacion.
    
    Retorna: (sql_query, parametros)
    """
    sql_query = """
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
                                WHEN YEAR(e.Fecha) = ? -- 1: anio
                                 AND MONTH(e.Fecha) = ? -- 2: mes
                                THEN e.Remision 
                            END) AS Embarques_Mes,
            ISNULL(SUM(e.KgTotal), 0) AS Kg_Historico,
            ISNULL(SUM(CASE 
                                WHEN YEAR(e.Fecha) = ? -- 3: anio
                                 AND MONTH(e.Fecha) = ? -- 4: mes
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
                                WHEN YEAR(e.Fecha) = ? -- 5: anio
                                 AND MONTH(e.Fecha) = ? -- 6: mes
                                THEN e.Remision 
                            END) AS Embarques_Mes,
            ISNULL(SUM(e.KgTotal), 0) AS Kg_Historico,
            ISNULL(SUM(CASE 
                                WHEN YEAR(e.Fecha) = ? -- 7: anio
                                 AND MONTH(e.Fecha) = ? -- 8: mes
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
    parametros = (anio, mes, anio, mes, anio, mes, anio, mes)
    return sql_query, parametros

def get_query_tendencia_anual(anio: int) -> tuple[str, tuple]:
    """Query 4: Kilos totales embarcados por mes para el año seleccionado.
    
    Retorna: (sql_query, parametros)
    """
    sql_query = """
    SELECT
        MONTH(e.Fecha) AS Mes,
        ROUND(SUM(e.KgTotal), 2) AS Total_Kg
    FROM (
        -- UNION de Embarques Estral y Cimsa para tener todos los datos
        SELECT Fecha, KgTotal FROM Embarques
        UNION ALL
        SELECT Fecha, KgTotal FROM CimsaEmbarques
    ) e
    WHERE YEAR(e.Fecha) = ? -- Placeholder 1: anio
    GROUP BY MONTH(e.Fecha)
    ORDER BY Mes ASC;
    """
    parametros = (anio,)
    return sql_query, parametros

# ----------------------------------------------------

# --- RUTA PRINCIPAL (CORREGIDA) ---

@router.get("/embarques", response_class=HTMLResponse)
def embarques_page(
    request: Request,
    access_token: str = Cookie(None),
    mes: int = Query(None),
    anio: int = Query(None)
):
    # 1. Validación de Acceso (Sin cambios)
    payload = validar_token_embarques(access_token)
    if not payload:
        return JSONResponse(status_code=403, content={"error": "Acceso denegado. No tienes permisos para esta sección."})

    # 2. Manejo de Parámetros de Tiempo (Sin cambios)
    today = datetime.now()
    if mes is None:
        mes = today.month
    if anio is None:
        anio = today.year

    try:
        # 3. Ejecución de Consultas (¡CAMBIOS AQUÍ!)
        # Desempaquetamos la consulta SQL y los parámetros
        sql_diarios, params_diarios = get_query_embarques_diarios(mes, anio)
        # Pasamos la consulta y los parámetros a la función
        data_diarios = ejecutar_consulta_sql(sql_diarios, params=params_diarios, fetchall=True)

        sql_progreso, params_progreso = get_query_progreso_mensual(mes, anio)
        data_progreso = ejecutar_consulta_sql(sql_progreso, params=params_progreso, fetchall=True)
        
        # Esto evita el error "TypeError: Object of type date is not JSON serializable" 
        for item in data_diarios:
            if 'DiaEmbarque' in item and isinstance(item['DiaEmbarque'], date):
                item['DiaEmbarque'] = item['DiaEmbarque'].strftime('%Y-%m-%d')

    except Exception as e:
        # Si la consulta a la BD falla, devolvemos el error 500
        return JSONResponse(status_code=500, content={"error": "Error al consultar la base de datos de Embarques.", "detail": str(e)})

    # 4. Preparación de Datos (Sin cambios)
    nombre_mes = calendar.month_name[mes].capitalize()
    
    progreso_total = next((item for item in data_progreso if item['Pedido'] == 'TOTAL MES'), {})
    progreso_detalle = [item for item in data_progreso if item['Pedido'] != 'TOTAL MES']


    # 5. Renderización de la Template (Sin cambios)
    return templates.TemplateResponse("embarques.html", {
        "request": request,
        "usuario": payload.get("sub", "Usuario"),
        "mes_actual": mes,
        "anio_actual": anio,
        "nombre_mes": nombre_mes,
        "anio": anio,
        "data_diarios": data_diarios, 
        "progreso_total": progreso_total,
        "progreso_detalle": progreso_detalle
    })

# ENDPOINT PARA LA GRÁFICA MENSUAL (CORREGIDO)
@router.get("/embarques/tendencia_anual", response_class=JSONResponse)
def tendencia_anual_endpoint(
    access_token: str = Cookie(None),
    anio: int = Query(None)
):
    # 1. Validación de Acceso (Sin cambios)
    payload = validar_token_embarques(access_token)
    if not payload:
        return JSONResponse(status_code=403, content={"error": "Acceso denegado."})

    # 2. Manejo de Parámetros de Tiempo (Sin cambios)
    if anio is None:
        return JSONResponse(status_code=400, content={"error": "El parámetro 'anio' es requerido."})

    try:
        # 3. Ejecución de la Consulta SQL (¡CAMBIOS AQUÍ!)
        sql_tendencia, params_tendencia = get_query_tendencia_anual(anio)
        
        # Ejecutar la consulta y obtener los resultados
        resultados = ejecutar_consulta_sql(sql_tendencia, params=params_tendencia, fetchall=True)
        
    except Exception as e:
        # 4. Manejo de errores de BD (Sin cambios)
        return JSONResponse(status_code=500, content={"error": "Error al consultar la tendencia anual.", "detail": str(e)})

    # 5. Devolver el JSON (Sin cambios)
    return JSONResponse(content={
        "tendencia_mensual": resultados
    })