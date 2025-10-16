from fastapi import APIRouter, Request, Cookie, Query, Depends, HTTPException
from fastapi.responses import JSONResponse, HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from utils.auth import verificar_access_token
from typing import Tuple, Dict, Any, List
from services.db_service import ejecutar_consulta_mysql 
from datetime import datetime

# Importaciones y configuraciones existentes
router = APIRouter()
templates = Jinja2Templates(directory="templates")
EMPLEADOS_PERMITIDOS_COTIZACIONES = [1000, 1001, 1002, 8811, 4, 5] 

# --- FUNCIONES DE SOPORTE DE ACCESO (Mantenemos la tuya) ---
def validar_acceso_cotizaciones(access_token: str = Cookie(None)):
    """
    Verifica el token, lo decodifica y valida que el K_Empleado esté en la lista de permitidos.
    """
    if not access_token:
        raise HTTPException(status_code=302, detail="No autorizado", headers={"Location": "/auth/login"})
        
    token = access_token.replace("Bearer ", "")
    payload = verificar_access_token(token)
    
    if not payload:
        raise HTTPException(status_code=302, detail="Token inválido", headers={"Location": "/auth/login"})
    
    k_empleado = payload.get("K_Empleado")
    
    if k_empleado in EMPLEADOS_PERMITIDOS_COTIZACIONES:
        return payload 
    
    raise HTTPException(status_code=403, detail="Acceso denegado. Permisos insuficientes.")

# --- ENDPOINTS ---


# ... (Tu código de imports y funciones de soporte va aquí) ...

# --- ENDPOINTS ---

# 1. Dashboard principal (Maneja /cotizaciones y /cotizaciones/)
@router.get("/", response_class=HTMLResponse, summary="Muestra el dashboard principal de cotizaciones")
@router.get("", response_class=HTMLResponse, include_in_schema=False)
async def dashboard_cotizaciones(
    request: Request,
    # Usa la dependencia para asegurar que el usuario esté logueado y tenga permisos
    usuario: Dict[str, Any] = Depends(validar_acceso_cotizaciones) 
):
    """
    Renderiza la plantilla HTML del dashboard de cotizaciones.
    """
    
    # Preparamos la fecha actual en formato YYYYMM para el valor por defecto en el input
    today_ym = datetime.now().strftime("%Y%m")
    
    return templates.TemplateResponse(
        "cotizaciones.html", 
        {
            "request": request,
            "usuario": usuario,        # Puedes usar esto para mostrar el nombre del empleado
            "today_ym": today_ym       # Fecha por defecto para el input
        }
    )

from decimal import Decimal

def decimales_a_float(data):
    """
    Recorre una lista de diccionarios y convierte los Decimal a float.
    """
    for row in data:
        for key, value in row.items():
            if isinstance(value, Decimal):
                row[key] = float(value)
    return data


@router.get("/resumen_metricas", 
            response_model=List[Dict[str, Any]],
            summary="Obtiene el resumen de métricas de cotizaciones por fecha.")
async def obtener_resumen_metricas(
    fecha: str = Query(..., 
                       regex=r"^\d{6}$", 
                       description="Fecha en formato YYYYMM (Ej: 202510)"), 
    usuario: Dict[str, Any] = Depends(validar_acceso_cotizaciones) 
) -> JSONResponse:
    
    parametro_sql_fecha = fecha[-4:]  # Mantén tu lógica de YYMM para la consulta

    SQL_QUERY = f"""
SELECT
    CASE
        WHEN GROUPING(Clasificacion) = 1 THEN 'TOTAL GENERAL'
        ELSE Clasificacion
    END AS Clasificacion,

    CASE
        WHEN GROUPING(Clasificacion) = 1 THEN
            (
                SELECT COUNT(DISTINCT q3.idQuotation)
                FROM quotation q3
                WHERE q3.quotationDate = {parametro_sql_fecha}
            )
        ELSE SUM(Total_Cotizaciones)
    END AS Total_Cotizaciones,

    CASE
        WHEN GROUPING(Clasificacion) = 1 THEN
            FORMAT(SUM(CASE
                            WHEN Clasificacion NOT IN ('Entregada a Tiempo', 'Entregada con Retraso', 'En Riesgo')
                            THEN Total_Kilos
                            ELSE 0
                        END), 2, 'es_MX')
        ELSE FORMAT(SUM(Total_Kilos), 2, 'es_MX')
    END AS Total_Kilos,

    CASE
        WHEN GROUPING(Clasificacion) = 1 THEN NULL
        WHEN Clasificacion IN ('Cerrada','Vendida') THEN FORMAT(SUM(Total_Monto), 2, 'es_MX')
        ELSE NULL
    END AS Total_Monto,

    CASE
        WHEN GROUPING(Clasificacion) = 1 THEN NULL
        ELSE FORMAT(AVG_PricePerKg, 2, 'es_MX')
    END AS Precio_Promedio_Kg

FROM (
    -- Aquí van tus subqueries de Abiertas, Cerradas, Entregadas, etc.
    SELECT 'Abierta' AS Clasificacion,
           COUNT(DISTINCT q.idQuotation) AS Total_Cotizaciones,
           SUM(qs.totalKgSold) AS Total_Kilos,
           0 AS Total_Monto,
           NULL AS AVG_PricePerKg
    FROM quotation q
    LEFT JOIN sale_status s ON q.saleStatus = s.idSalestatus
    LEFT JOIN status st ON q.status = st.idStatus
    LEFT JOIN quotation_systems qs ON q.idQuotation = qs.quotationId
    WHERE q.quotationDate = {parametro_sql_fecha}
      AND s.Nombe <> 'Vendido'
      AND st.status NOT IN ('Finalizada', 'Finalizada con retraso', 'Finalizada con retraso de ventas', 'En riesgo')

    UNION ALL

    -- Cerradas
    SELECT 'Cerrada' AS Clasificacion,
           COUNT(DISTINCT q.idQuotation) AS Total_Cotizaciones,
           SUM(qs.totalKgSold) AS Total_Kilos,
           SUM(qs.totalPrice) AS Total_Monto,
           NULL AS AVG_PricePerKg
    FROM quotation q
    LEFT JOIN sale_status s ON q.saleStatus = s.idSalestatus
    LEFT JOIN status st ON q.status = st.idStatus
    LEFT JOIN quotation_systems qs ON q.idQuotation = qs.quotationId
    WHERE q.quotationDate = {parametro_sql_fecha}
      AND (s.Nombe = 'Vendido' OR st.status IN ('Finalizada', 'Finalizada con retraso', 'Finalizada con retraso de ventas'))

    UNION ALL

    -- Entregadas a Tiempo
    SELECT 'Entregada a Tiempo' AS Clasificacion,
           COUNT(DISTINCT q.idQuotation) AS Total_Cotizaciones,
           SUM(qs.totalKgSold) AS Total_Kilos,
           0 AS Total_Monto,
           NULL AS AVG_PricePerKg
    FROM quotation q
    LEFT JOIN status st ON q.status = st.idStatus
    LEFT JOIN quotation_systems qs ON q.idQuotation = qs.quotationId
    WHERE q.quotationDate = {parametro_sql_fecha}
      AND st.status = 'Finalizada'

    UNION ALL

    -- Entregadas con Retraso
    SELECT 'Entregada con Retraso' AS Clasificacion,
           COUNT(DISTINCT q.idQuotation) AS Total_Cotizaciones,
           SUM(qs.totalKgSold) AS Total_Kilos,
           0 AS Total_Monto,
           NULL AS AVG_PricePerKg
    FROM quotation q
    LEFT JOIN status st ON q.status = st.idStatus
    LEFT JOIN quotation_systems qs ON q.idQuotation = qs.quotationId
    WHERE q.quotationDate = {parametro_sql_fecha}
      AND st.status IN ('Finalizada con retraso', 'Finalizada con retraso de ventas')

    UNION ALL

    -- En Riesgo
    SELECT 'En Riesgo' AS Clasificacion,
           COUNT(DISTINCT q.idQuotation) AS Total_Cotizaciones,
           SUM(qs.totalKgSold) AS Total_Kilos,
           0 AS Total_Monto,
           NULL AS AVG_PricePerKg
    FROM quotation q
    LEFT JOIN status st ON q.status = st.idStatus
    LEFT JOIN quotation_systems qs ON q.idQuotation = qs.quotationId
    WHERE q.quotationDate = {parametro_sql_fecha}
      AND st.status = 'En riesgo'

    UNION ALL

    -- Vendidas
    SELECT 'Vendida' AS Clasificacion,
           COUNT(DISTINCT q.idQuotation) AS Total_Cotizaciones,
           SUM(qs.totalKgSold) AS Total_Kilos,
           SUM(qs.totalPrice) AS Total_Monto,
           (SELECT AVG(NULLIF(ROUND(qs2.pricePerKg,2),0))
            FROM quotation q2
            LEFT JOIN sale_status s2 ON q2.saleStatus = s2.idSalestatus
            LEFT JOIN status st2 ON q2.status = st2.idStatus
            LEFT JOIN quotation_systems qs2 ON q2.idQuotation = qs2.quotationId
            WHERE q2.quotationDate = {parametro_sql_fecha}
              AND (s2.Nombe = 'Vendido' OR st2.status IN ('Finalizada','Finalizada con retraso','Finalizada con retraso de ventas'))
              AND qs2.pricePerKg IS NOT NULL AND qs2.pricePerKg <> 0
           ) AS AVG_PricePerKg
    FROM quotation q
    LEFT JOIN sale_status s ON q.saleStatus = s.idSalestatus
    LEFT JOIN status st ON q.status = st.idStatus
    LEFT JOIN quotation_systems qs ON q.idQuotation = qs.quotationId
    WHERE q.quotationDate = {parametro_sql_fecha}
      AND s.Nombe = 'Vendido'
) AS resumen
GROUP BY Clasificacion WITH ROLLUP
ORDER BY
    CASE
        WHEN Clasificacion = 'Abierta' THEN 1
        WHEN Clasificacion = 'Cerrada' THEN 2
        WHEN Clasificacion = 'Entregada a Tiempo' THEN 3
        WHEN Clasificacion = 'Entregada con Retraso' THEN 4
        WHEN Clasificacion = 'En Riesgo' THEN 5
        WHEN Clasificacion = 'Vendida' THEN 6
        WHEN Clasificacion = 'TOTAL GENERAL' THEN 7
        ELSE 8
    END;
    """

    try:
        resultados = ejecutar_consulta_mysql(SQL_QUERY, fetchall=True)
        resultados = decimales_a_float(resultados)
        return JSONResponse(content=resultados or [], status_code=200)
    except Exception as e:
        print(f"Error al ejecutar la consulta de cotizaciones: {e}")
        raise HTTPException(status_code=500, detail="Error interno al obtener los datos de la base de datos.")
