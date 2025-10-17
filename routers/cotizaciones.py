from fastapi import APIRouter, Request, Cookie, Query, Depends, HTTPException
from fastapi.responses import JSONResponse, HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from utils.auth import verificar_access_token
from typing import Dict, Any, List
from services.db_service import ejecutar_consulta_mysql 
from datetime import datetime
from decimal import Decimal
from fastapi.encoders import jsonable_encoder
import json # Necesario para manejar la serialización de datos

# =================================================================
# CONFIGURACIÓN INICIAL
# =================================================================

router = APIRouter()
templates = Jinja2Templates(directory="templates")
EMPLEADOS_PERMITIDOS_COTIZACIONES = [1000, 1001, 1002, 8811, 4, 5] 

# =================================================================
# FUNCIONES DE SOPORTE
# =================================================================

def validar_acceso_cotizaciones(access_token: str = Cookie(None)):
    """
    Verifica el token, lo decodifica y valida que el K_Empleado esté en la lista de permitidos.
    """
    if not access_token:
        # 302: Redirección. FastAPI maneja esto con el header Location
        raise HTTPException(status_code=302, detail="No autorizado", headers={"Location": "/auth/login"})
        
    token = access_token.replace("Bearer ", "")
    payload = verificar_access_token(token)
    
    if not payload:
        raise HTTPException(status_code=302, detail="Token inválido", headers={"Location": "/auth/login"})
    
    k_empleado = payload.get("K_Empleado")
    
    if k_empleado in EMPLEADOS_PERMITIDOS_COTIZACIONES:
        return payload 
    
    raise HTTPException(status_code=403, detail="Acceso denegado. Permisos insuficientes.")

def decimales_a_float(data):
    """
    Recorre una lista de diccionarios y convierte los Decimal a float de forma segura.
    """
    if not isinstance(data, list):
        return data

    for row in data:
        for key, value in row.items():
            if isinstance(value, Decimal):
                row[key] = float(value)
    return data

# =================================================================
# ENDPOINTS
# =================================================================

# 1. Dashboard principal (HTML)
@router.get("/", response_class=HTMLResponse, summary="Muestra el dashboard principal de cotizaciones")
@router.get("", response_class=HTMLResponse, include_in_schema=False)
async def dashboard_cotizaciones(
    request: Request,
    usuario: Dict[str, Any] = Depends(validar_acceso_cotizaciones) 
):
    """
    Renderiza la plantilla HTML del dashboard de cotizaciones.
    """
    
    today_ym = datetime.now().strftime("%Y%m")
    
    return templates.TemplateResponse(
        "cotizaciones.html", 
        {
            "request": request,
            "usuario": usuario,
            "today_ym": today_ym
        }
    )

# 2. Resumen de Métricas
@router.get("/resumen_metricas", 
            response_model=List[Dict[str, Any]],
            summary="Obtiene el resumen de métricas de cotizaciones por fecha.")
async def obtener_resumen_metricas(
    fecha: str = Query(..., 
                        regex=r"^\d{6}$", 
                        description="Fecha en formato YYYYMM (Ej: 202510)"), 
    usuario: Dict[str, Any] = Depends(validar_acceso_cotizaciones) 
) -> JSONResponse:
    
    # ⚠️ Seguridad: El valor inyectado en el SQL es solo el número de YYMM
    parametro_sql_fecha = fecha[-4:]

    # El SQL usa el placeholder %s. Se usa la interpolación del cliente de BD, no f-strings
    # OJO: Por la complejidad del WITH ROLLUP y los subselects, se tiene que inyectar el valor.
    # Una solución más segura sería usar un Stored Procedure. Dejamos el f-string con la sanitización básica.
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
    -- Subqueries (todos usan {parametro_sql_fecha} para filtrar)
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
        # Aquí no se pasan parámetros, solo el query (ya inyectado, pero validado)
        resultados = ejecutar_consulta_mysql(SQL_QUERY, fetchall=True)
        resultados = decimales_a_float(resultados)
        return JSONResponse(content=resultados or [], status_code=200)
    except Exception as e:
        print(f"Error al ejecutar la consulta de cotizaciones: {e}")
        raise HTTPException(status_code=500, detail="Error interno al obtener los datos de la base de datos.")

# 3. Detalle de Cerradas (SEGURO: usa %s)
@router.get(
    "/detalle_cerradas",
    response_model=List[Dict[str, Any]],
    summary="Detalle de cotizaciones finalizadas con info visual"
)
async def detalle_cerradas(
    fecha: str = Query(..., regex=r"^\d{6}$", description="Fecha en formato YYYYMM"),
    usuario: Dict[str, Any] = Depends(validar_acceso_cotizaciones)
):
    """
    Retorna todas las cotizaciones cerradas (finalizadas o vendidas) para el mes especificado,
    con info visual (colores e íconos) para mejorar el dashboard.
    """
    # ⚠️ Seguridad: Usamos el placeholder %s para el filtro
    parametro_sql_fecha = fecha[-4:] 

    SQL_QUERY = """
    SELECT 
        q.`name` AS Nombre_Cotizacion,
        CONCAT(q.`quotationDate`, '-', q.`quotationConsecutive`) AS Folio_Cotizacion,
        ROUND(qs.`totalKgSold`, 2) AS totalKgSold,
        ROUND(qs.`totalPrice`, 2) AS totalPrice,
        ROUND(qs.`pricePerKg`, 2) AS pricePerKg,
        CASE 
            WHEN qs.`pricePerKg` IS NULL OR qs.`pricePerKg` = 0 THEN '⚠️ Sin precio'
            ELSE '✅ Con precio'
        END AS Estado_Precio,
        s.`Nombe` AS Estatus_Venta,
        st.`status` AS Estatus_Tecnico
    FROM `quotation` q
    LEFT JOIN `sale_status` s ON q.`saleStatus` = s.`idSalestatus`
    LEFT JOIN `status` st ON q.`status` = st.`idStatus`
    LEFT JOIN `quotation_systems` qs ON q.`idQuotation` = qs.`quotationId`
    WHERE q.`quotationDate` = %s
      AND (
            s.`Nombe` = 'Vendido'
            OR st.`status` IN ('Finalizada', 'Finalizada con retraso', 'Finalizada con retraso de ventas')
          )
    ORDER BY qs.`pricePerKg` DESC;
    """

    try:
        # Pasar el valor como parámetro
        params = (parametro_sql_fecha,)
        resultados = ejecutar_consulta_mysql(SQL_QUERY, params=params, fetchall=True)

        # Agregamos info visual
        for r in resultados:
            # Color según estado de precio
            r['color_estado'] = 'red' if r.get('Estado_Precio') == '⚠️ Sin precio' else 'green'
            r['icono_estado'] = '⚠️' if r.get('Estado_Precio') == '⚠️ Sin precio' else '✅'

            # Color según estatus técnico
            estatus = r.get('Estatus_Tecnico', '').lower()
            if 'retraso' in estatus:
                r['color_tecnico'] = 'orange'
            elif 'finalizada' in estatus:
                r['color_tecnico'] = 'blue'
            elif 'riesgo' in estatus:
                r['color_tecnico'] = 'red'
            else:
                r['color_tecnico'] = 'grey'

        return resultados if resultados else []

    except Exception as e:
        print(f"Error al obtener detalle de cotizaciones: {e}")
        raise HTTPException(status_code=500, detail="Error interno al obtener los datos")
    


# 4. Lista Completa (SEGURO: usa %s)
@router.get(
    "/lista",
    response_model=List[Dict[str, Any]],
    summary="Lista de cotizaciones filtradas por mes y año (versión segura)"
)
async def listar_cotizaciones(
    fecha: str = Query(..., regex=r"^\d{6}$", description="Fecha en formato YYYYMM (Ej: 202510)"),
    usuario: Dict[str, Any] = Depends(validar_acceso_cotizaciones)
) -> JSONResponse:
    """
    Devuelve el detalle completo de cotizaciones para un mes y año específico, 
    utilizando consultas parametrizadas y conversión segura a JSON.
    """
    try:
        # Validación de formato YYYYMM
        if len(fecha) != 6:
            raise HTTPException(status_code=400, detail="Formato de fecha inválido, debe ser YYYYMM")

        # 🚨 CAMBIO 1: Extraer solo 'AAMM' para el filtro (Ej: '2510')
        # Esto asume que quotationDate solo guarda los últimos 2 dígitos del año + el mes.
        quotation_date_aamm = fecha[2:]  

        # SQL parametrizada
        SQL_QUERY = """
        SELECT
            q.idQuotation,
            q.name AS quotation_name,
            q.quotationDate,
            q.quotationConsecutive,
            q.createdAt,
            q.percentage,
            q.step,
            COALESCE(s.Nombe, 'Sin estatus') AS Estatus_Venta,
            COALESCE(st.status, 'Sin estatus') AS Estado,
            CASE
                -- 1. VENDIDA: saleStatus = 2
                WHEN q.saleStatus = 2 THEN 'Vendida' 
                -- 2. ENTREGADA A TIEMPO: status = 8
                WHEN q.status = 8 THEN 'Entregada a Tiempo'
                -- 3. ENTREGADA CON RETRASO: status = 7 o 10
                WHEN q.status IN (7, 10) THEN 'Entregada con Retraso'
                -- 4. EN RIESGO: status = 5
                WHEN q.status = 5 THEN 'En Riesgo'
                -- 5. CERRADA: q.deliver no es nulo
                WHEN q.deliver IS NOT NULL THEN 'Cerrada'
                -- 6. POR DEFECTO
                ELSE 'Abierta'
            END AS Clasificacion,
            q.deliver
        FROM quotation q
        LEFT JOIN sale_status s ON q.saleStatus = s.idSalestatus
        LEFT JOIN status st ON q.status = st.idStatus
        -- 🚨 CAMBIO 2: Filtrar por quotationDate usando el valor AAMM
        WHERE q.quotationDate = %s
        ORDER BY
            FIELD(
                CASE
                    WHEN q.saleStatus = 2 THEN 'Vendida'
                    WHEN q.status = 8 THEN 'Entregada a Tiempo'
                    WHEN q.status IN (7, 10) THEN 'Entregada con Retraso'
                    WHEN q.status = 5 THEN 'En Riesgo'
                    WHEN q.deliver IS NOT NULL THEN 'Cerrada'
                    ELSE 'Abierta'
                END,
                'Vendida', 'Entregada a Tiempo', 'Entregada con Retraso', 'En Riesgo', 'Cerrada', 'Abierta'
            ),
            q.createdAt DESC;
        """

        # 🚨 CAMBIO 3: Usar solo un parámetro, el valor 'AAMM'
        params = (quotation_date_aamm,) 

        # Ejecuta la consulta
        resultados = ejecutar_consulta_mysql(SQL_QUERY, params=params, fetchall=True) or []

        # Convierte decimales a float si aplica
        resultados = decimales_a_float(resultados)

        # Convierte todos los valores None y datetime a tipos JSON válidos
        for row in resultados:
            for key, value in row.items():
                if value is None:
                    row[key] = ""
                elif isinstance(value, datetime):
                    row[key] = value.strftime("%Y-%m-%d %H:%M:%S")

        # Usa el encoder de FastAPI para serializar correctamente
        return JSONResponse(content=jsonable_encoder(resultados), status_code=200)

    except HTTPException:
        raise
    except Exception as e:
        print(f"[ERROR /lista] {e}")
        raise HTTPException(status_code=500, detail=f"Error interno al obtener las cotizaciones: {str(e)}")



# 5. Detalle Universal por Clasificación
@router.get(
    "/detalle_por_clasificacion",
    response_model=List[Dict[str, Any]],
    summary="Detalle de cotizaciones filtrado por Clasificación (Abierta, Riesgo, etc.)"
)
async def detalle_por_clasificacion(
    fecha: str = Query(..., regex=r"^\d{6}$", description="Fecha en formato YYYYMM"),
    clasificacion: str = Query(..., description="Clasificación a filtrar (ej: 'En Riesgo')"),
    usuario: Dict[str, Any] = Depends(validar_acceso_cotizaciones)
):
    """
    Retorna las cotizaciones que coinciden con una Clasificación específica.
    Usa el filtro de quotationDate y la lógica de clasificación por ID, eliminando duplicados con GROUP BY.
    """

    # 1. Validar y preparar el filtro de fecha (AAMM)
    try:
        if len(fecha) != 6:
            raise HTTPException(status_code=400, detail="Formato de fecha inválido")
        # Extraemos AAMM para el filtro (ej: '2510' de '202510')
        quotation_date_aamm = fecha[2:] 
    except (ValueError, IndexError):
        raise HTTPException(status_code=400, detail="Formato de fecha inválido")


    # Paso 2: Obtener los IDs de cotización que cumplen con el filtro de fecha y la Clasificación
    SQL_ID_QUERY = """
        SELECT
            q.idQuotation
        FROM quotation q
        LEFT JOIN sale_status s ON q.saleStatus = s.idSalestatus
        LEFT JOIN status st ON q.status = st.idStatus
        WHERE q.quotationDate = %s
          AND CASE
              WHEN q.saleStatus = 2 THEN 'Vendida'
              WHEN q.status = 8 THEN 'Entregada a Tiempo'
              WHEN q.status IN (7, 10) THEN 'Entregada con Retraso'
              WHEN q.status = 5 THEN 'En Riesgo'
              WHEN q.deliver IS NOT NULL THEN 'Cerrada'
              ELSE 'Abierta'
          END = %s;
    """

    params_ids = (quotation_date_aamm, clasificacion)

    try:
        resultados_ids = ejecutar_consulta_mysql(SQL_ID_QUERY, params=params_ids, fetchall=True)
        if not resultados_ids:
            return JSONResponse(content=[], status_code=200)

        quotation_ids = [str(r['idQuotation']) for r in resultados_ids if r.get('idQuotation')]
        if not quotation_ids:
            return JSONResponse(content=[], status_code=200)

        id_list = ",".join(quotation_ids)

    except Exception as e:
        print(f"Error al obtener IDs por clasificación: {e}")
        raise HTTPException(status_code=500, detail="Error interno al obtener IDs de cotizaciones.")

    # Paso 3: Obtener el detalle de la cotización usando los IDs (con GROUP BY y SUM)
    SQL_DETALLE_QUERY = f"""
        SELECT 
            q.name AS Nombre_Cotizacion,
            CONCAT(q.quotationDate, '-', q.quotationConsecutive) AS Folio_Cotizacion,
            -- Usamos SUM para consolidar los totales
            ROUND(SUM(qs.totalKgSold), 2) AS totalKgSold,
            ROUND(SUM(qs.totalPrice), 2) AS totalPrice,
            -- Recalculamos el precio por Kg
            ROUND(SUM(qs.totalPrice) / NULLIF(SUM(qs.totalKgSold), 0), 2) AS pricePerKg,
            CASE 
                WHEN SUM(qs.totalKgSold) IS NULL OR SUM(qs.totalPrice) = 0 THEN '⚠️ Sin precio'
                ELSE '✅ Con precio'
            END AS Estado_Precio,
            s.Nombe AS Estatus_Venta,
            st.status AS Estatus_Tecnico
        FROM quotation q
        LEFT JOIN sale_status s ON q.saleStatus = s.idSalestatus
        LEFT JOIN status st ON q.status = st.idStatus
        LEFT JOIN quotation_systems qs ON q.idQuotation = qs.quotationId
        WHERE q.idQuotation IN ({id_list})
        
        -- Agrupamos por los campos no agregados para obtener una sola fila por cotización
        GROUP BY 
            q.idQuotation, 
            q.name, 
            Folio_Cotizacion, 
            s.Nombe, 
            st.status,
            q.quotationDate,
            q.quotationConsecutive
            
        ORDER BY totalKgSold DESC;
    """

    try:
        resultados = ejecutar_consulta_mysql(SQL_DETALLE_QUERY, fetchall=True)
        resultados = decimales_a_float(resultados)

        return JSONResponse(
            content=jsonable_encoder(resultados) if resultados else [],
            status_code=200
        )

    except Exception as e:
        print(f"Error al obtener detalle de cotizaciones por clasificación: {e}")
        raise HTTPException(status_code=500, detail="Error interno al obtener los datos del detalle.")

