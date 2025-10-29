# routers/asignacion_equipos.py
from datetime import datetime
from fastapi import APIRouter, Request, Form, Cookie
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from utils.auth import verificar_access_token
from services.db_service import ejecutar_consulta_sql

router = APIRouter()
templates = Jinja2Templates(directory="templates")

# ---- CONTROL DE ACCESO ----
AREAS_PERMITIDAS_ASIGNACION = [20, 22]  # ajusta según tus áreas
EMPLEADOS_PERMITIDOS_ASIGNACION = [8709, 8827, 7854, 4, 5, 8679]

def validar_acceso_asignacion(payload: dict) -> bool:
    if not payload:
        return False
    k_empleado = payload.get("K_Empleado")
    k_area = payload.get("K_Area")
    return (k_area in AREAS_PERMITIDAS_ASIGNACION) or (k_empleado in EMPLEADOS_PERMITIDOS_ASIGNACION)

def get_payload_from_cookie(access_token: str = Cookie(None)):
    if not access_token:
        return None
    token = access_token.replace("Bearer ", "")
    return verificar_access_token(token)


# ---- PÁGINA PRINCIPAL ----
@router.get("/", response_class=HTMLResponse)
async def asignacion_equipos(request: Request, access_token: str = Cookie(None)):
    payload = get_payload_from_cookie(access_token)
    if not validar_acceso_asignacion(payload):
        return HTMLResponse(
            "<h2 style='color:red;text-align:center;margin-top:40px;'>⛔ Acceso denegado</h2>",
            status_code=403
        )

    # Consultas principales
    sql_stock = """
        SELECT id, codigo, tipo, area, estatus, responsable, fecha_actualizacion, observaciones
        FROM ws_equipos
        WHERE estatus = 'Sin asignar'
        ORDER BY area
    """
    sql_asignados = """
        SELECT id, codigo, tipo, area, estatus, responsable, fecha_asignacion, fecha_actualizacion, observaciones
        FROM ws_equipos
        WHERE estatus IN ('Operando', 'En reparación', 'Fuera de servicio')
        ORDER BY area
    """

    try:
        equipos_stock = ejecutar_consulta_sql(sql_stock, fetchall=True) or []
        equipos_asignados = ejecutar_consulta_sql(sql_asignados, fetchall=True) or []

        # ---------------- KPIs ----------------
        sql_totales_tipo = """
            SELECT tipo, COUNT(*) as total
            FROM ws_equipos
            GROUP BY tipo
        """
        res_tipo = ejecutar_consulta_sql(sql_totales_tipo, fetchall=True) or []

        total_handheld = next((r['total'] for r in res_tipo if r['tipo'] == 'Handheld'), 0)
        total_impresoras = next((r['total'] for r in res_tipo if r['tipo'] == 'Impresora portátil'), 0)
        total_tablets = next((r['total'] for r in res_tipo if r['tipo'] == 'Tablet'), 0)


        # Total por área
        sql_por_area = """
            SELECT area, COUNT(*) as total
            FROM ws_equipos
            GROUP BY area
        """
        res_area = ejecutar_consulta_sql(sql_por_area, fetchall=True) or []
        total_por_area = {r['area'] or 'NA': r['total'] for r in res_area}

        # Equipos fuera de servicio
        sql_fuera = "SELECT COUNT(*) as total FROM ws_equipos WHERE estatus = 'Fuera de servicio'"
        total_fuera_servicio = ejecutar_consulta_sql(sql_fuera, fetchone=True)['total'] or 0

        # Equipos asignados funcionando y dañados por área
        sql_status_area = """
            SELECT area,
                   SUM(CASE WHEN estatus='Operando' THEN 1 ELSE 0 END) as funcionando,
                   SUM(CASE WHEN estatus='Fuera de servicio' THEN 1 ELSE 0 END) as dañados
            FROM ws_equipos
            GROUP BY area
        """
        res_status_area = ejecutar_consulta_sql(sql_status_area, fetchall=True) or []
        status_por_area = {r['area'] or 'NA': {'funcionando': r['funcionando'], 'dañados': r['dañados']} for r in res_status_area}

    except Exception as ex:
        return HTMLResponse(
            "<h2 style='color:red;text-align:center;margin-top:40px;'>Error al leer equipos</h2>",
            status_code=500
        )

    return templates.TemplateResponse("asignacion_equipos.html", {
        "request": request,
        "equipos_stock": equipos_stock,
        "equipos_asignados": equipos_asignados,
        "usuario": payload.get("Nombre") if payload else "Desconocido",
        "total_handheld": total_handheld,
        "total_impresoras": total_impresoras,
        "total_tablets": total_tablets,   # <- agregado
        "total_fuera_servicio": total_fuera_servicio,
        "total_por_area": total_por_area,
        "status_por_area": status_por_area
    })



# NUEVO EQUIPO
@router.post("/nuevo", response_class=JSONResponse)
async def nuevo_equipo(
    codigo: str = Form(...),
    tipo: str = Form(...),
    area: str = Form(...),
    estatus: str = Form(...),
    responsable: str = Form(None),
    observaciones: str = Form(None),
    modelo_hand: str = Form(None),
    access_token: str = Cookie(None)
):
    payload = get_payload_from_cookie(access_token)
    if not validar_acceso_asignacion(payload):
        return JSONResponse(status_code=403, content={"status": "error", "msg": "Acceso denegado"})

    if tipo != "Handheld":
        modelo_hand = None

    # Verificar duplicado
    existente = ejecutar_consulta_sql("SELECT TOP 1 id FROM ws_equipos WHERE codigo = ?", (codigo,), fetchone=True)
    if existente:
        return JSONResponse(status_code=409, content={"status": "error", "msg": f"El equipo con código '{codigo}' ya existe."})

    # Insert
    insert_sql = """
        INSERT INTO ws_equipos
        (codigo, tipo, modelo_hand, area, estatus, responsable, fecha_asignacion, fecha_actualizacion, observaciones)
        VALUES (?, ?, ?, ?, ?, ?, GETDATE(), GETDATE(), ?)
    """
    ejecutar_consulta_sql(insert_sql, (codigo, tipo, modelo_hand, area, estatus, responsable, observaciones))
    return JSONResponse({"status": "ok", "msg": "Equipo guardado con éxito"})


@router.post("/editar", response_class=JSONResponse)
async def editar_equipo(
    id: int = Form(...),
    tipo: str = Form(...),
    estatus: str = Form(...),
    area: str = Form(...),
    responsable: str = Form(None),
    observaciones: str = Form(None),
    modelo_hand: str | None = Form(None),  # opcional
    access_token: str = Cookie(None)
):
    payload = get_payload_from_cookie(access_token)
    if not validar_acceso_asignacion(payload):
        return JSONResponse(status_code=403, content={"status": "error", "msg": "Acceso denegado"})

    if tipo != "Handheld":
        modelo_hand = None

    update_sql = """
        UPDATE ws_equipos
        SET estatus=?, area=?, responsable=?, observaciones=?, modelo_hand=?, fecha_actualizacion=GETDATE()
        WHERE id=?
    """
    ejecutar_consulta_sql(update_sql, (estatus, area, responsable, observaciones, modelo_hand, id))

    sql_historial = """
        INSERT INTO ws_historial_equipos (equipo_id, accion, motivo, usuario, fecha)
        VALUES (?, 'Actualización', 'Edición de información del equipo', ?, GETDATE())
    """
    usuario = payload.get("Nombre") if payload else "sistema"
    ejecutar_consulta_sql(sql_historial, (id, usuario))

    return JSONResponse({"status": "ok", "msg": "Equipo actualizado correctamente"})



# ---- ACTUALIZAR SOLO FECHA ----
@router.post("/refrescar_fecha", response_class=JSONResponse)
async def refrescar_fecha(equipo_id: int = Form(...), access_token: str = Cookie(None)):
    payload = get_payload_from_cookie(access_token)
    if not validar_acceso_asignacion(payload):
        return JSONResponse(status_code=403, content={"status": "error", "msg": "Acceso denegado"})

    try:
        ejecutar_consulta_sql("UPDATE ws_equipos SET fecha_actualizacion = GETDATE() WHERE id = ?", (equipo_id,))
        sql_historial = """
            INSERT INTO ws_historial_equipos (equipo_id, accion, motivo, usuario, fecha)
            VALUES (?, 'Actualización rápida', 'Solo se actualizó la fecha de revisión', ?, GETDATE())
        """
        usuario = payload.get("Nombre") if payload else "sistema"
        ejecutar_consulta_sql(sql_historial, (equipo_id, usuario))
    except Exception as ex:
        return JSONResponse(status_code=500, content={"status": "error", "msg": "Error al refrescar fecha"})

    return JSONResponse(content={"status": "ok", "msg": "Fecha de actualización registrada"})


# ---- EDICIÓN COMPLETA ----
@router.post("/editar_completo", response_class=JSONResponse)
async def editar_completo_equipo(
    id: int = Form(...),
    codigo: str = Form(...),
    tipo: str = Form(...),
    modelo_hand: str | None = Form(None),
    estatus: str = Form(...),
    area: str = Form(...),
    responsable: str = Form(None),
    observaciones: str = Form(None),
    access_token: str = Cookie(None)
):
    payload = get_payload_from_cookie(access_token)
    if not validar_acceso_asignacion(payload):
        return JSONResponse(status_code=403, content={"status": "error", "msg": "Acceso denegado"})

    if tipo != "Handheld":
        modelo_hand = None

    sql = """
        UPDATE ws_equipos
        SET codigo=?, tipo=?, modelo_hand=?, estatus=?, area=?, responsable=?, observaciones=?, fecha_actualizacion=GETDATE()
        WHERE id=?
    """
    ejecutar_consulta_sql(sql, (codigo, tipo, modelo_hand, estatus, area, responsable, observaciones, id))

    usuario = payload.get("Nombre") if payload else "sistema"
    ejecutar_consulta_sql(
        "INSERT INTO ws_historial_equipos (equipo_id, accion, motivo, usuario, fecha) VALUES (?, 'Edición completa', 'Modificación total de datos', ?, GETDATE())",
        (id, usuario)
    )

    return JSONResponse({"status": "ok", "msg": "Equipo actualizado correctamente."})


# ---- ELIMINAR EQUIPO ----
@router.post("/eliminar", response_class=JSONResponse)
async def eliminar_equipo(id: int = Form(...), access_token: str = Cookie(None)):
    payload = get_payload_from_cookie(access_token)
    if not validar_acceso_asignacion(payload):
        return JSONResponse(status_code=403, content={"status": "error", "msg": "Acceso denegado"})

    try:
        usuario = payload.get("Nombre") if payload else "sistema"

        # 1️⃣ Borrar historial del equipo
        ejecutar_consulta_sql("DELETE FROM ws_historial_equipos WHERE equipo_id = ?", (id,))

        # 2️⃣ Borrar equipo
        ejecutar_consulta_sql("DELETE FROM ws_equipos WHERE id = ?", (id,))

        # 3️⃣ Registrar la acción en historial (opcional, aunque ya borraste el historial antiguo)
        ejecutar_consulta_sql(
            "INSERT INTO ws_historial_equipos (equipo_id, accion, motivo, usuario, fecha) VALUES (?, 'Eliminación', 'Registro eliminado del sistema', ?, GETDATE())",
            (id, usuario)
        )

        return JSONResponse({"status": "ok", "msg": "Equipo eliminado correctamente."})

    except Exception as ex:
        print(ex)  # útil para debug
        return JSONResponse(status_code=500, content={"status": "error", "msg": "Error al eliminar equipo."})
