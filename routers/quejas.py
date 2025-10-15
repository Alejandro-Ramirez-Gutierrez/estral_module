# routers/quejas.py

from fastapi import APIRouter, Request, Cookie, Query
from fastapi.responses import JSONResponse, HTMLResponse, Response
from fastapi.templating import Jinja2Templates
from utils.auth import verificar_access_token # Asegúrate de que esta ruta sea correcta
from services.db_service import ejecutar_consulta_sql # Asegúrate de que esta ruta sea correcta
from datetime import datetime, date
from io import BytesIO
import openpyxl # Asegúrate de tener: pip install openpyxl

router = APIRouter()
templates = Jinja2Templates(directory="templates")

# Permisos: Solo los empleados con estos K_Empleado tendrán acceso al dashboard de Quejas
EMPLEADOS_PERMITIDOS = [8811, 8661, 8870, 8740, 4, 5]

# --- FUNCIONES DE SOPORTE DE ACCESO ---

def validar_token_quejas(access_token: str):
    """Verifica el token y el K_Empleado contra la lista de permitidos."""
    if not access_token:
        return None
    token = access_token.replace("Bearer ", "")
    payload = verificar_access_token(token)
    if not payload:
        return None
    
    # Validamos que el empleado esté en la lista de permitidos
    if payload.get("K_Empleado") in EMPLEADOS_PERMITIDOS:
        return payload
    return None

def get_payload_from_cookie(access_token: str = Cookie(None)):
    """Extrae el payload del token sin validar permisos."""
    if not access_token:
        return None
    token = access_token.replace("Bearer ", "")
    return verificar_access_token(token)

def validar_acceso_quejas(payload: dict) -> bool:
    """Lógica simple para validar que haya un payload válido."""
    if not payload:
        return False
    # En un entorno real, aquí iría una lógica más robusta si fuera necesario,
    # pero para este caso, la validación principal es validar_token_quejas.
    return True 

# --- LÓGICA SQL: Construcción de Consultas (FINAL) ---

def get_detalle_quejas_query(mes: int, anio: int) -> str:
    """Retorna el SQL para obtener el detalle de todas las quejas."""
    
    # 1. Base del WHERE: Siempre filtra por Pedido_Estral '%S%' o '%Q%'
    where_clausula = "(p.Pedido_Estral LIKE '%S%' OR p.Pedido_Estral LIKE '%Q%')"
    
    # 2. Si el mes NO es 0, añade el filtro de Fecha
    if mes != 0:
        where_clausula += f" AND YEAR(p.Fecha) = {anio} AND MONTH(p.Fecha) = {mes}"

    return f"""
    SELECT
        p.Pedido_Estral,
        FORMAT(p.Fecha, 'dd/MM/yyyy') AS Fecha_Formato,
        d.Destinatario AS Cliente,
        q.Tipo_Referencia,
        ROUND(ISNULL(q.Kg_Queja, 0), 2) AS Kg_Queja,
        ROUND(ISNULL(prod.Kg_Produccion, 0), 2) AS Kg_Produccion,
        ISNULL(q.Pzas_Queja, 0) AS Pzas_Queja,
        ISNULL(prod.Pzas_Produccion, 0) AS Pzas_Produccion,
        CASE
            WHEN q.K_Pedido IS NULL AND ISNULL(prod.Kg_Produccion, 0) = 0 AND ISNULL(prod.Pzas_Produccion, 0) = 0 THEN 'PENDIENTE DE INGENIERIA'
            WHEN q.K_Pedido IS NOT NULL AND ISNULL(prod.Kg_Produccion, 0) = 0 AND ISNULL(prod.Pzas_Produccion, 0) = 0 THEN 'ACTIVA (SIN AVANCE)'
            WHEN q.K_Pedido IS NOT NULL AND (ISNULL(prod.Kg_Produccion, 0) > 0 OR ISNULL(prod.Pzas_Produccion, 0) > 0) THEN
                CASE
                    WHEN q.Tipo_Referencia = 'R' AND ISNULL(prod.Pzas_Produccion, 0) >= ISNULL(q.Pzas_Queja, 0) THEN 'CERRADA'
                    WHEN q.Tipo_Referencia = 'R' AND ISNULL(prod.Pzas_Produccion, 0) < ISNULL(q.Pzas_Queja, 0) THEN 'EN PROCESO'
                    WHEN q.Tipo_Referencia IN ('L','E','P','S') AND ABS(ISNULL(prod.Kg_Produccion, 0) - ISNULL(q.Kg_Queja, 0)) < 0.01 THEN 'CERRADA'
                    WHEN q.Tipo_Referencia IN ('L','E','P','S') AND ISNULL(prod.Kg_Produccion, 0) < ISNULL(q.Kg_Queja, 0) THEN 'EN PROCESO'
                    ELSE 'CERRADA'
                END
            ELSE 'EN PROCESO'
        END AS Estatus_Queja
    FROM Pedidos p
    INNER JOIN Domicilio_Pedido d ON p.K_Pedido = d.K_Pedido
    LEFT JOIN (SELECT K_Pedido, MAX(Material) AS Tipo_Referencia, SUM(KgTotal) AS Kg_Queja, SUM(Cantidad) AS Pzas_Queja FROM Partidas GROUP BY K_Pedido) q ON p.K_Pedido = q.K_Pedido
    LEFT JOIN (SELECT Pedido, SUM(KgTotal) AS Kg_Produccion, SUM(Cantidad) AS Pzas_Produccion FROM Produccion WHERE Area IN ('ENSAMBLE','CIMSA/ ENSAMBLE','HABILITADO','PERFILADO','ALMACEN') GROUP BY Pedido) prod ON p.Pedido_Estral = prod.Pedido
    WHERE
        {where_clausula} 
    ORDER BY p.Fecha DESC;
    """

def get_resumen_quejas_query(mes: int, anio: int) -> str:
    """Retorna el SQL para obtener el resumen (contadores) de quejas."""
    
    # 1. Cláusula WHERE para los subqueries (filtrando por fecha si mes != 0)
    where_clausula_sub = "(p.Pedido_Estral LIKE '%S%' OR p.Pedido_Estral LIKE '%Q%')"
    if mes != 0:
        where_clausula_sub += f" AND YEAR(p.Fecha) = {anio} AND MONTH(p.Fecha) = {mes}"
        
    # 2. Cláusula WHERE para el TOTAL general (filtrando por fecha si mes != 0)
    where_clausula_total = "(Pedido_Estral LIKE '%S%' OR Pedido_Estral LIKE '%Q%')"
    if mes != 0:
        where_clausula_total += f" AND YEAR(Fecha) = {anio} AND MONTH(Fecha) = {mes}"

    return f"""
    SELECT Estatus_Queja, Total
    FROM (
        SELECT Estatus_Queja, Total,
            CASE WHEN Estatus_Queja = 'TOTAL QUEJAS MES' THEN 2 ELSE 1 END AS Orden
        FROM (
            SELECT
                Estatus_Queja,
                COUNT(*) AS Total
            FROM (
                SELECT
                    p.Pedido_Estral,
                    CASE
                        WHEN q.K_Pedido IS NULL AND ISNULL(prod.Kg_Produccion, 0) = 0 AND ISNULL(prod.Pzas_Produccion, 0) = 0 THEN 'PENDIENTE DE INGENIERIA'
                        WHEN q.K_Pedido IS NOT NULL AND ISNULL(prod.Kg_Produccion, 0) = 0 AND ISNULL(prod.Pzas_Produccion, 0) = 0 THEN 'ACTIVA (SIN AVANCE)'
                        WHEN q.K_Pedido IS NOT NULL AND (ISNULL(prod.Kg_Produccion, 0) > 0 OR ISNULL(prod.Pzas_Produccion, 0) > 0) THEN
                            CASE
                                WHEN q.Tipo_Referencia = 'R' AND ISNULL(prod.Pzas_Produccion, 0) >= ISNULL(q.Pzas_Queja, 0) THEN 'CERRADA'
                                WHEN q.Tipo_Referencia = 'R' AND ISNULL(prod.Pzas_Produccion, 0) < ISNULL(q.Pzas_Queja, 0) THEN 'EN PROCESO'
                                WHEN q.Tipo_Referencia IN ('L','E','P','S') AND ABS(ISNULL(prod.Kg_Produccion, 0) - ISNULL(q.Kg_Queja, 0)) < 0.01 THEN 'CERRADA'
                                WHEN q.Tipo_Referencia IN ('L','E','P','S') AND ISNULL(prod.Kg_Produccion, 0) < ISNULL(q.Kg_Queja, 0) THEN 'EN PROCESO'
                                ELSE 'CERRADA'
                            END
                        ELSE 'EN PROCESO'
                    END AS Estatus_Queja
                FROM Pedidos p
                INNER JOIN Domicilio_Pedido d ON p.K_Pedido = d.K_Pedido
                LEFT JOIN (SELECT K_Pedido, MAX(Material) AS Tipo_Referencia, SUM(KgTotal) AS Kg_Queja, SUM(Cantidad) AS Pzas_Queja FROM Partidas GROUP BY K_Pedido) q ON p.K_Pedido = q.K_Pedido
                LEFT JOIN (SELECT Pedido, SUM(KgTotal) AS Kg_Produccion, SUM(Cantidad) AS Pzas_Produccion FROM Produccion WHERE Area IN ('ENSAMBLE','CIMSA/ ENSAMBLE','HABILITADO','PERFILADO','ALMACEN') GROUP BY Pedido) prod ON p.Pedido_Estral = prod.Pedido
                WHERE
                    {where_clausula_sub}
            ) AS t
            GROUP BY Estatus_Queja
            UNION ALL
            SELECT
                'TOTAL QUEJAS MES' AS Estatus_Queja,
                COUNT(*) AS Total
            FROM Pedidos
            WHERE
                {where_clausula_total}
        ) u
    ) final
    ORDER BY final.Orden, final.Estatus_Queja;
    """

# --- RUTA PRINCIPAL (GET /quejas) ---

@router.get("/", response_class=HTMLResponse)
def quejas_page(
    request: Request,
    access_token: str = Cookie(None),
    mes: int = Query(None),
    anio: int = Query(None)
):
    # 1. Validación de Acceso
    payload = validar_token_quejas(access_token)
    if not payload:
        return JSONResponse(status_code=403, content={"error": "Acceso denegado. No tienes permisos para esta sección."})

    # 2. Manejo de Parámetros de Tiempo (Mes y Año)
    today = datetime.now()
    if mes is None:
        mes = today.month # Mes actual por defecto
    if anio is None:
        anio = today.year
    
    # 3. Ejecución de Consultas
    try:
        sql_resumen = get_resumen_quejas_query(mes, anio)
        resumen_data = ejecutar_consulta_sql(sql_resumen, fetchall=True)
        
        sql_detalle = get_detalle_quejas_query(mes, anio)
        detalle_data = ejecutar_consulta_sql(sql_detalle, fetchall=True)

    except Exception as e:
        return JSONResponse(status_code=500, content={"error": "Error al consultar la base de datos.", "detail": str(e)})

    # 4. Preparación de Datos para la Template
    resumen_dict = {item['Estatus_Queja']: item['Total'] for item in resumen_data}
    
    # Lógica para el título (cambia si mes=0)
    if mes == 0:
        nombre_mes_anio = "Todas las Quejas Históricas"
    else:
        try:
            meses = {1: 'Enero', 2: 'Febrero', 3: 'Marzo', 4: 'Abril', 5: 'Mayo', 6: 'Junio', 7: 'Julio', 8: 'Agosto', 9: 'Septiembre', 10: 'Octubre', 11: 'Noviembre', 12: 'Diciembre'}
            nombre_mes = meses.get(mes, "Mes Inválido")
            nombre_mes_anio = f"{nombre_mes} de {anio}"
        except Exception:
            nombre_mes_anio = "Periodo Inválido"

    # 5. Renderización de la Template
    return templates.TemplateResponse("quejas.html", {
        "request": request,
        "usuario": payload.get("sub", "Usuario"),
        "mes_actual": mes,
        "anio_actual": anio,
        "nombre_mes_anio": nombre_mes_anio,
        "resumen": resumen_dict,
        "detalle": detalle_data
    })

# --- RUTA DE EXPORTACIÓN A EXCEL (GET /quejas/exportar) ---

@router.get("/exportar", response_class=Response)
def exportar_quejas_excel(
    access_token: str = Cookie(None),
    mes: int = Query(None),
    anio: int = Query(None)
):
    # 1. Validación de Acceso
    payload = validar_token_quejas(access_token)
    if not payload:
        return JSONResponse(status_code=403, content={"error": "Acceso denegado."})

    # 2. Manejo de Parámetros de Tiempo
    today = datetime.now()
    if mes is None:
        mes = today.month
    if anio is None:
        anio = today.year
    
    # 3. Ejecución de Consulta
    try:
        # Usa la misma lógica: si mes=0, trae todo
        sql_detalle = get_detalle_quejas_query(mes, anio) 
        detalle_data = ejecutar_consulta_sql(sql_detalle, fetchall=True)

    except Exception as e:
        return JSONResponse(status_code=500, content={"error": "Error al consultar la base de datos para exportar."})
    
    if not detalle_data:
        return JSONResponse(status_code=404, content={"message": "No hay datos para exportar en el periodo seleccionado."})

    # 4. Generación del Archivo Excel en Memoria
    output = BytesIO()
    workbook = openpyxl.Workbook()
    sheet = workbook.active
    
    headers = list(detalle_data[0].keys())
    sheet.append(headers)

    for row in detalle_data:
        sheet.append([row[h] for h in headers])

    workbook.save(output)
    output.seek(0)
    
    # 5. Configurar la respuesta de descarga
    if mes == 0:
        filename = f"Reporte_Quejas_Historico_{today.strftime('%Y%m%d')}.xlsx"
    else:
        meses = {1: 'Enero', 2: 'Febrero', 3: 'Marzo', 4: 'Abril', 5: 'Mayo', 6: 'Junio', 7: 'Julio', 8: 'Agosto', 9: 'Septiembre', 10: 'Octubre', 11: 'Noviembre', 12: 'Diciembre'}
        nombre_mes = meses.get(mes, "Mes")
        filename = f"Reporte_Quejas_{nombre_mes}_{anio}.xlsx"

    return Response(
        content=output.read(),
        media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        headers={
            'Content-Disposition': f'attachment; filename="{filename}"'
        }
    )

# --- RUTA DE HISTORIAL POR PEDIDO (GET /quejas/historial) ---
@router.get("/historial")
def historial_pedido(pedido: str, access_token: str = Cookie(None)):
    
    # 1. Validación de acceso
    if not validar_token_quejas(access_token):
        return JSONResponse(status_code=403, content={"error": "Acceso denegado"})

    # 2. Consulta SQL segura
    # El query usa '?' para la sustitución de parámetros.
    query = """
    SELECT
        m.tipo AS TIPO,
        m.pedido AS PEDIDO,
        m.partida AS PARTIDA,
        m.descripcion AS DESCRIPCION,
        m.color AS COLOR,
        m.cantidad AS CANTIDAD,
        
        -- Producción
        ISNULL(SUM(CASE WHEN p.Area IN ('ENSAMBLE','PERFILADO','HABILITADO','CIMSA/ ENSAMBLE') 
                         THEN p.Cantidad END),0) AS ENSAMBLE,
        ISNULL(SUM(
            CASE 
                WHEN p.Area IN ('PINTURA','pintura plan','CIMSA/ PINTURA')
                         OR (p.Area IN ('ENSAMBLE','PERFILADO','HABILITADO','CIMSA/ ENSAMBLE')
                              AND (p.Color LIKE '%GALVANIZADO%' OR p.Color LIKE '%GALV%' OR p.Color LIKE '%SIN%'))
                THEN p.Cantidad 
                ELSE 0 
            END
        ),0) AS PINTURA,

        -- Embarques
        ISNULL(r.CantidadRecibida,0) - ISNULL(e.CantidadEmbarcada,0) AS PATIO,
        ISNULL(e.CantidadEmbarcada,0) AS EMBARQUE,

        -- Flags para debug
        CASE WHEN SUM(p.Cantidad) IS NULL THEN 0 ELSE 1 END AS ExisteEnProduccion,
        CASE WHEN e.CantidadEmbarcada IS NULL AND r.CantidadRecibida IS NULL THEN 0 ELSE 1 END AS ExisteEnEmbarques

    FROM Mostrar m
    LEFT JOIN Produccion p
        ON m.pedido = p.Pedido AND m.partida = p.Partida
    LEFT JOIN (
        SELECT x.Pedido, x.Partida, SUM(x.CantidadEmbarcada) AS CantidadEmbarcada
        FROM (
            SELECT Pedido, Partida, CantidadEmbarcada FROM embarques
            UNION ALL
            SELECT Pedido, Partida, CantidadEmbarcada FROM CIMSAEMBARQUES
        ) x
        GROUP BY x.Pedido, x.Partida
    ) e ON m.pedido = e.Pedido AND m.partida = e.Partida
    LEFT JOIN (
        SELECT Pedido, Partida, SUM(CantidadRecibida) AS CantidadRecibida
        FROM EmbarquesMaterialRecibido 
        GROUP BY Pedido, Partida 
    ) r ON m.pedido = r.Pedido AND m.partida = r.Partida

    WHERE m.pedido = ? 
    GROUP BY m.tipo, m.pedido, m.partida, m.descripcion, m.color, m.cantidad, e.CantidadEmbarcada, r.CantidadRecibida
    ORDER BY
        TRY_CAST(LEFT(m.partida, PATINDEX('%[^0-9]%', m.partida + 'X') - 1) AS INT),
        RIGHT(m.partida, LEN(m.partida) - PATINDEX('%[^0-9]%', m.partida + 'X') + 1);
    """

    # 3. Ejecución con manejo de errores (¡LLamada correcta!)
    try:
        # ✅ Se pasa el parámetro (pedido,) como el segundo argumento (params)
        rows = ejecutar_consulta_sql(query, (pedido,), fetchall=True) or []
    except Exception as ex:
        import traceback
        print("ERROR HISTORIAL:", traceback.format_exc())
        return JSONResponse(status_code=500, content={"error": f"Error al consultar historial: {str(ex)}"  })


    # 4. Normalización y respuesta
    out = []
    for r in rows:
        item = dict(r)
        # Convierte flags a booleanos
        item["ExisteEnProduccion"] = bool(item.get("ExisteEnProduccion"))
        item["ExisteEnEmbarques"] = bool(item.get("ExisteEnEmbarques"))
        out.append(item)

    return {"pedido": pedido, "historial": out}