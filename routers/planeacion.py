# routers/planeacion.py
import re
from datetime import datetime, date
from fastapi import APIRouter, Request, Form, Cookie
from fastapi.responses import JSONResponse, HTMLResponse
from fastapi.templating import Jinja2Templates

from utils.auth import verificar_access_token
from services.db_service import ejecutar_consulta_sql

router = APIRouter()
templates = Jinja2Templates(directory="templates")

# Ajusta las listas a quienes quieres que vean/usen Planeación
AREAS_PERMITIDAS_PLANEACION = [20, 22]          
EMPLEADOS_PERMITIDOS_PLANEACION = [8811, 8661, 8870, 8740,4,5]  

def validar_acceso_planeacion(payload: dict) -> bool:
    if not payload:
        return False
    k_empleado = payload.get("K_Empleado")
    k_area = payload.get("K_Area")
    return (k_area in AREAS_PERMITIDAS_PLANEACION) or (k_empleado in EMPLEADOS_PERMITIDOS_PLANEACION)

def get_payload_from_cookie(access_token: str = Cookie(None)):
    if not access_token:
        return None
    token = access_token.replace("Bearer ", "")
    return verificar_access_token(token)

# Página HTML del dashboard de planeación (si quieres usar template)
@router.get("/", response_class=HTMLResponse)
def planeacion_page(request: Request, access_token: str = Cookie(None)):
    payload = get_payload_from_cookie(access_token)
    if not validar_acceso_planeacion(payload):
        return JSONResponse(status_code=403, content={"error": "Acceso denegado"})
    usuario = payload.get("sub", "Usuario")
    return templates.TemplateResponse("planeacion.html", {"request": request, "usuario": usuario})

# API: Listar lo que hay en WS_Planeacion
@router.get("/list")
def listar_planeacion(access_token: str = Cookie(None)):
    payload = get_payload_from_cookie(access_token)
    if not validar_acceso_planeacion(payload):
        return JSONResponse(status_code=403, content={"error": "Acceso denegado"})
    rows = ejecutar_consulta_sql("SELECT * FROM WS_Planeacion ORDER BY Fecha_Agregado DESC", fetchall=True) or []
    # Normalizar fechas/decimales para JSON
    out = []
    for r in rows:
        item = dict(r)
        # convierto objetos datetime/date a string si vienen así
        for fld in ("Fecha_Entrega","Fecha_Cierre","Fecha_Agregado"):
            v = item.get(fld)
            if isinstance(v, (datetime, date)):
                item[fld] = v.strftime("%Y-%m-%d %H:%M:%S") if isinstance(v, datetime) else v.strftime("%Y-%m-%d")
            elif v is None:
                item[fld] = None
        # decimales a float
        for fld in ("KGS","ENSAMBLE","PINTURA","EMBARQUE","KGS_Faltantes"):
            if item.get(fld) is not None:
                try:
                    item[fld] = float(item[fld])
                except:
                    pass
        out.append(item)
    return {"planeacion": out}

# API: Agregar un pedido (solo 1) - valida que exista con tu query y lo inserta en WS_Planeacion
@router.post("/add")
def agregar_pedido(pedido: str = Form(...), access_token: str = Cookie(None)):
    payload = get_payload_from_cookie(access_token)
    if not validar_acceso_planeacion(payload):
        return JSONResponse(status_code=403, content={"error": "Acceso denegado"})

    # Validación simple del formato del pedido (evita inyección básica)
    if not re.match(r'^[A-Za-z0-9\-]+$', pedido):
        return JSONResponse(status_code=400, content={"error": "Formato de pedido inválido"})

    # Evitamos duplicados en WS_Planeacion
    existente = ejecutar_consulta_sql(f"SELECT TOP 1 * FROM WS_Planeacion WHERE Pedido = '{pedido}'", fetchone=True)
    if existente:
        return JSONResponse(status_code=409, content={"error": "Pedido ya agregado en planeación"})

    # Query para obtener datos del pedido
    query = f"""
    ;WITH PedidosFiltro AS (
        SELECT '{pedido}' AS Pedido
    ),
    PedidosCTE AS (
        SELECT
            p.K_Pedido,
            p.Pedido_Estral AS Pedido,
            d.Destinatario AS Cliente,
            ts.D_Tipo_Sistema AS Sistema,
            SUM(m.cantidad * m.pesoUnitario) AS Kg_Programados,
            p.K_Estado_Pedido AS Status,
            p.F_Entrega AS Fecha_Entrega
        FROM db_Estral.dbo.Pedidos p
        LEFT JOIN db_Estral.dbo.Domicilio_Pedido d
            ON p.K_Pedido = d.K_Pedido
        LEFT JOIN db_Estral.dbo.Tipo_Sistema ts
            ON p.K_Tipo_Sistema = ts.K_Tipo_Sistema
        LEFT JOIN db_Estral.dbo.Mostrar m
            ON p.Pedido_Estral = m.pedido
        WHERE p.Pedido_Estral IN (SELECT Pedido FROM PedidosFiltro)
        GROUP BY p.K_Pedido, p.Pedido_Estral, d.Destinatario, ts.D_Tipo_Sistema, p.K_Estado_Pedido, p.F_Entrega
    ),
    ProduccionHist AS (
        SELECT
            Pedido,
            SUM(CASE WHEN Area IN ('ENSAMBLE','PERFILADO','HABILITADO','CIMSA/ ENSAMBLE')
                     THEN KgTotal ELSE 0 END) AS Kg_Ensamble,
            SUM(CASE 
                    WHEN Area IN ('PINTURA','pintura plan','CIMSA/ PINTURA') 
                         OR (Area IN ('ENSAMBLE','PERFILADO','HABILITADO','CIMSA/ ENSAMBLE') 
                             AND (Color LIKE '%GALVANIZADO%' OR Color LIKE '%GALV%' OR Color = 'SIN'))
                    THEN KgTotal ELSE 0 
                END) AS Kg_Pintura
        FROM db_Estral.dbo.Produccion
        WHERE Pedido IN (SELECT Pedido FROM PedidosFiltro)
        GROUP BY Pedido
    ),
    EmbarquesHist AS (
        SELECT
            Pedido,
            SUM(KgTotal) AS Kg_Embarque
        FROM (
            SELECT Pedido, KgTotal FROM db_Estral.dbo.embarques
            WHERE Pedido IN (SELECT Pedido FROM PedidosFiltro)
            UNION ALL
            SELECT Pedido, KgTotal FROM db_Estral.dbo.CIMSAEMBARQUES
            WHERE Pedido IN (SELECT Pedido FROM PedidosFiltro)
        ) x
        GROUP BY Pedido
    ),
    UltimaFecha AS (
        SELECT Pedido, MAX(Fecha) AS Fecha_Cierre
        FROM db_Estral.dbo.Produccion
        WHERE Pedido IN (SELECT Pedido FROM PedidosFiltro)
        GROUP BY Pedido
    )
    SELECT
        ped.Pedido,
        ped.Cliente,
        ped.Sistema,
        ped.Kg_Programados AS KGS,
        ROUND(
            ISNULL(
                CASE 
                    WHEN ISNULL(ph.Kg_Ensamble,0) >= ped.Kg_Programados 
                        THEN 100
                    ELSE (ph.Kg_Ensamble / NULLIF(ped.Kg_Programados,0) * 100)
                END,0
            ),1
        ) AS ENSAMBLE,
        ROUND(
            ISNULL(
                CASE 
                    WHEN ISNULL(ph.Kg_Pintura,0) >= ped.Kg_Programados 
                        THEN 100
                    ELSE (ph.Kg_Pintura / NULLIF(ped.Kg_Programados,0) * 100)
                END,0
            ),1
        ) AS PINTURA,
        ROUND(
            ISNULL(
                CASE 
                    WHEN ISNULL(e.Kg_Embarque,0) >= ped.Kg_Programados 
                        THEN 100
                    ELSE (e.Kg_Embarque / NULLIF(ped.Kg_Programados,0) * 100)
                END,0
            ),1
        ) AS EMBARQUE,
        ped.Status,
        CASE 
            WHEN ped.Kg_Programados - ISNULL(ph.Kg_Ensamble,0) < 0 
                THEN 0
            ELSE ped.Kg_Programados - ISNULL(ph.Kg_Ensamble,0) 
        END AS [KGS FALTANTES],
        ped.Fecha_Entrega,
        CASE 
            WHEN ROUND(
                    ISNULL(
                        CASE 
                            WHEN ISNULL(ph.Kg_Pintura,0) >= ped.Kg_Programados 
                                THEN 100
                            ELSE (ph.Kg_Pintura / NULLIF(ped.Kg_Programados,0) * 100)
                        END,0
                    ),1
                ) >= 99.9
            THEN uf.Fecha_Cierre
            ELSE NULL
        END AS Fecha_Cierre
    FROM PedidosCTE ped
    LEFT JOIN ProduccionHist ph
        ON ped.Pedido = ph.Pedido
    LEFT JOIN EmbarquesHist e
        ON ped.Pedido = e.Pedido
    LEFT JOIN UltimaFecha uf
        ON ped.Pedido = uf.Pedido;
    """

    # Ejecuta la validación/lectura del pedido
    try:
        resultado = ejecutar_consulta_sql(query, fetchone=True)
    except Exception as ex:
        return JSONResponse(status_code=500, content={"error": f"Error al ejecutar consulta de validación: {str(ex)}"})

    if not resultado:
        return JSONResponse(status_code=404, content={"error": "El pedido no existe o no tiene datos disponibles"})

    # Preparar valores para insert (escapar strings, dejar números sin comillas)
    pedido_val = str(resultado.get("Pedido") or pedido).replace("'", "''")
    cliente_val = (resultado.get("Cliente") or "").replace("'", "''")
    sistema_val = (resultado.get("Sistema") or "").replace("'", "''")
    kgs_val = resultado.get("KGS")
    ensamble_val = resultado.get("ENSAMBLE")
    pintura_val = resultado.get("PINTURA")
    embarque_val = resultado.get("EMBARQUE")
    status_val = resultado.get("Status") if resultado.get("Status") is not None else "NULL"
    kgs_faltantes_val = resultado.get("KGS FALTANTES") or resultado.get("KGS_FALTANTES") or resultado.get("KGS Faltantes") or "NULL"
    fecha_entrega_val = resultado.get("Fecha_Entrega")
    fecha_cierre_val = resultado.get("Fecha_Cierre")
    usuario_agrego = payload.get("sub", "unknown").replace("'", "''")

    def date_to_sql(d):
        if d is None:
            return "NULL"
        if isinstance(d, (datetime, date)):
            return f"'{d.strftime('%Y-%m-%d')}'"
        try:
            return f"'{str(d)[:10]}'"
        except:
            return "NULL"

    fecha_entrega_sql = date_to_sql(fecha_entrega_val)
    fecha_cierre_sql = date_to_sql(fecha_cierre_val)

    insert_sql = f"""
    INSERT INTO WS_Planeacion
    (Pedido, Cliente, Sistema, KGS, ENSAMBLE, PINTURA, EMBARQUE, Status, KGS_Faltantes, Fecha_Entrega, Fecha_Cierre, Usuario_Agrego)
    VALUES
    ('{pedido_val}', '{cliente_val}', '{sistema_val}', {kgs_val if kgs_val is not None else 'NULL'},
     {ensamble_val if ensamble_val is not None else 'NULL'}, {pintura_val if pintura_val is not None else 'NULL'},
     {embarque_val if embarque_val is not None else 'NULL'}, {status_val},
     {kgs_faltantes_val if kgs_faltantes_val != "NULL" else "NULL"},
     {fecha_entrega_sql}, {fecha_cierre_sql}, '{usuario_agrego}');
    """

    try:
        ejecutar_consulta_sql(insert_sql)
    except Exception as ex:
        return JSONResponse(status_code=500, content={"error": f"Error al insertar en WS_Planeacion: {str(ex)}"})

    # Traer la fila insertada
    inserted = ejecutar_consulta_sql(
        f"SELECT TOP 1 * FROM WS_Planeacion WHERE Pedido = '{pedido_val}' ORDER BY Fecha_Agregado DESC", fetchone=True
    )

    # Normalizar fechas/decimales
    if inserted:
        for fld in ("Fecha_Entrega","Fecha_Cierre","Fecha_Agregado"):
            v = inserted.get(fld)
            if isinstance(v, (datetime, date)):
                inserted[fld] = v.strftime("%Y-%m-%d %H:%M:%S") if isinstance(v, datetime) else v.strftime("%Y-%m-%d")
        for fld in ("KGS","ENSAMBLE","PINTURA","EMBARQUE","KGS_FALTANTES"):
            if inserted.get(fld) is not None:
                try:
                    inserted[fld] = float(inserted[fld])
                except:
                    pass

    return {"message": "Pedido agregado a planeación", "inserted": inserted}

# API: Borrar pedido de WS_Planeacion
@router.delete("/{pedido}")
def borrar_pedido(pedido: str, access_token: str = Cookie(None)):
    payload = get_payload_from_cookie(access_token)
    if not validar_acceso_planeacion(payload):
        return JSONResponse(status_code=403, content={"error": "Acceso denegado"})

    # comprobar existencia
    existente = ejecutar_consulta_sql(f"SELECT TOP 1 * FROM WS_Planeacion WHERE Pedido = '{pedido}'", fetchone=True)
    if not existente:
        return JSONResponse(status_code=404, content={"error": "No existe ese pedido en planeación"})

    try:
        ejecutar_consulta_sql(f"DELETE FROM WS_Planeacion WHERE Pedido = '{pedido}'")
    except Exception as ex:
        return JSONResponse(status_code=500, content={"error": f"Error al eliminar pedido: {str(ex)}"})

    return {"message": f"Pedido {pedido} eliminado de planeación"}


# API: Listar pedidos de producción con estado de completado
@router.get("/list_noprogramados")
def listar_no_programados(mes: int = None, anio: int = None, access_token: str = Cookie(None)):
    payload = get_payload_from_cookie(access_token)
    if not validar_acceso_planeacion(payload):
        return JSONResponse(status_code=403, content={"error": "Acceso denegado"})

    hoy = datetime.today()
    if not mes:
        mes = hoy.month - 1 if hoy.month > 1 else 12
    if not anio:
        anio = hoy.year if mes != 12 else hoy.year - 1

    query = f"""
    ;WITH ProduccionMes AS (
        SELECT 
            Pedido,
            SUM(KgTotal) AS KgTotal,
            SUM(CASE WHEN Area IN ('ENSAMBLE','PERFILADO','HABILITADO','CIMSA/ ENSAMBLE') THEN KgTotal ELSE 0 END) AS Kg_Ensamble,
            SUM(CASE WHEN Area IN ('PINTURA','pintura plan','CIMSA/ PINTURA') 
                     OR (Area IN ('ENSAMBLE','PERFILADO','HABILITADO','CIMSA/ ENSAMBLE') AND (Color LIKE '%GALVANIZADO%' OR Color LIKE '%GALV%')) 
                     THEN KgTotal ELSE 0 END) AS Kg_Pintura,
            SUM(CASE WHEN Area LIKE '%EMBARQUE%' THEN KgTotal ELSE 0 END) AS Kg_Embarque
        FROM db_Estral.dbo.Produccion
        WHERE YEAR(Fecha) = {anio} AND MONTH(Fecha) = {mes}
        GROUP BY Pedido
    ),
    PedidosHistorico AS (
        SELECT
            p.Pedido_Estral AS Pedido,
            SUM(m.cantidad * m.pesoUnitario) AS Kg_Programados
        FROM db_Estral.dbo.Pedidos p
        LEFT JOIN db_Estral.dbo.Mostrar m ON p.Pedido_Estral = m.pedido
        GROUP BY p.Pedido_Estral
    ),
    ProduccionHist AS (
        SELECT
            Pedido,
            SUM(CASE WHEN Area IN ('ENSAMBLE','PERFILADO','HABILITADO','CIMSA/ ENSAMBLE') THEN KgTotal ELSE 0 END) AS Kg_Ensamble,
            SUM(CASE WHEN Area IN ('PINTURA','pintura plan','CIMSA/ PINTURA') 
                     OR (Area IN ('ENSAMBLE','PERFILADO','HABILITADO','CIMSA/ ENSAMBLE') AND (Color LIKE '%GALVANIZADO%' OR Color LIKE '%GALV%' OR Color='SIN'))
                     THEN KgTotal ELSE 0 END) AS Kg_Pintura
        FROM db_Estral.dbo.Produccion
        GROUP BY Pedido
    ),
    EmbarquesHist AS (
        SELECT
            Pedido,
            SUM(KgTotal) AS Kg_Embarque
        FROM (
            SELECT Pedido, KgTotal FROM db_Estral.dbo.embarques
            UNION ALL
            SELECT Pedido, KgTotal FROM db_Estral.dbo.CIMSAEMBARQUES
        ) x
        GROUP BY Pedido
    )
    SELECT 
        pm.Pedido,
        p.Pedido_Estral AS Pedido_Estral,
        d.Destinatario AS Cliente,
        ts.D_Tipo_Sistema AS Sistema,
        pm.KgTotal,
        pm.Kg_Ensamble,
        pm.Kg_Pintura,
        pm.Kg_Embarque,
        MAX(pr.Fecha) AS Ultima_Fecha,
        CASE
            WHEN wp.ENSAMBLE = 100 AND wp.PINTURA = 100 AND wp.EMBARQUE = 100 THEN 'Completado'
            WHEN ROUND(ISNULL(ph.Kg_Ensamble,0)/NULLIF(h.Kg_Programados,0)*100,1) >= 99.9
             AND ROUND(ISNULL(ph.Kg_Pintura,0)/NULLIF(h.Kg_Programados,0)*100,1) >= 99.9
             AND ROUND(ISNULL(e.Kg_Embarque,0)/NULLIF(h.Kg_Programados,0)*100,1) >= 99.9 THEN 'Completado'
            ELSE 'Pendiente'
        END AS Estado
    FROM ProduccionMes pm
    INNER JOIN db_Estral.dbo.Pedidos p ON pm.Pedido = p.Pedido_Estral
    LEFT JOIN db_Estral.dbo.Domicilio_Pedido d ON p.K_Pedido = d.K_Pedido
    LEFT JOIN db_Estral.dbo.Tipo_Sistema ts ON p.K_Tipo_Sistema = ts.K_Tipo_Sistema
    LEFT JOIN db_Estral.dbo.Produccion pr ON pm.Pedido = pr.Pedido
    LEFT JOIN WS_Planeacion wp ON pm.Pedido = wp.Pedido
    LEFT JOIN PedidosHistorico h ON pm.Pedido = h.Pedido
    LEFT JOIN ProduccionHist ph ON pm.Pedido = ph.Pedido
    LEFT JOIN EmbarquesHist e ON pm.Pedido = e.Pedido
    GROUP BY pm.Pedido, p.Pedido_Estral, d.Destinatario, ts.D_Tipo_Sistema,
             pm.KgTotal, pm.Kg_Ensamble, pm.Kg_Pintura, pm.Kg_Embarque,
             wp.ENSAMBLE, wp.PINTURA, wp.EMBARQUE, ph.Kg_Ensamble, ph.Kg_Pintura, e.Kg_Embarque, h.Kg_Programados
    ORDER BY Estado DESC, Ultima_Fecha DESC;
    """

    rows = ejecutar_consulta_sql(query, fetchall=True) or []

    out = []
    for r in rows:
        item = dict(r)
        for fld in ("KgTotal", "Kg_Ensamble", "Kg_Pintura", "Kg_Embarque"):
            if item.get(fld) is not None:
                item[fld] = float(item[fld])
        if isinstance(item.get("Ultima_Fecha"), (datetime, date)):
            item["Ultima_Fecha"] = item["Ultima_Fecha"].strftime("%Y-%m-%d")
        out.append(item)

    return {
        "anio": anio,
        "mes": mes,
        "pedidos": out
    }
