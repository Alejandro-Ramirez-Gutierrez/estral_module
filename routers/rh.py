from fastapi import APIRouter, Request, Cookie, Form
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from utils.auth import verificar_access_token
from services.db_service import ejecutar_consulta_sql

router = APIRouter()
templates = Jinja2Templates(directory="templates")


# -------------------- DASHBOARD RH --------------------
@router.get("/", response_class=HTMLResponse)
def dashboard_rh(request: Request, access_token: str = Cookie(None)):
    if not access_token:
        return JSONResponse(status_code=401, content={"error": "No autorizado"})

    token = access_token.replace("Bearer ", "")
    payload = verificar_access_token(token)
    if not payload or payload.get("K_Area") != 20:
        return JSONResponse(status_code=403, content={"error": "Acceso denegado"})

    return templates.TemplateResponse("rh.html", {"request": request, "user": payload})


# -------------------- OBTENER VACANTES DISPONIBLES (CORREGIDO) --------------------
@router.get("/vacantes_disponibles", response_class=JSONResponse)
def obtener_vacantes_disponibles(access_token: str = Cookie(None)):
    # ... (Verificación de seguridad)

    query = """
        SELECT 
            p.id_plantilla,
            e.id_planta,        -- <<-- ¡CAMBIO CLAVE: AGREGAR id_planta!
            e.nombre_planta,
            p.departamento,
            p.nombre_puesto,
            p.tipo_funcion,
            p.tipo_empleado,
            p.plantilla_autorizada,
            p.empleados_activos,
            p.vacantes_disponibles
        FROM ws_rh_PuestosPlantilla AS p
        INNER JOIN ws_rh_EmpresasPlantas AS e ON e.id_planta = p.id_planta
        WHERE p.vacantes_disponibles > 0
        ORDER BY e.nombre_planta, p.departamento, p.nombre_puesto
    """
    vacantes = ejecutar_consulta_sql(query, fetchall=True)
    return JSONResponse(content=vacantes)


# -------------------- REGISTRAR EMPLEADO --------------------
@router.post("/registrar_empleado", response_class=JSONResponse)
def registrar_empleado(
    nombre_completo: str = Form(...),
    apellido_paterno: str = Form(...),
    apellido_materno: str = Form(None),
    curp: str = Form(...),
    nss: str = Form(...),
    rfc: str = Form(...),
    fecha_nacimiento: str = Form(...),
    id_plantilla: int = Form(...),
    salario_diario: float = Form(...),
    tipo_empleado: str = Form(...),
    access_token: str = Cookie(None)
):
    if not access_token:
        return JSONResponse(status_code=401, content={"error": "No autorizado"})

    token = access_token.replace("Bearer ", "")
    payload = verificar_access_token(token)
    if not payload or payload.get("K_Area") != 20:
        return JSONResponse(status_code=403, content={"error": "Acceso denegado"})

    # ✅ Verificar si hay vacantes disponibles
    query_check = """
        SELECT plantilla_autorizada, empleados_activos
        FROM ws_rh_PuestosPlantilla
        WHERE id_plantilla = ?
    """
    vacante = ejecutar_consulta_sql(query_check, params=(id_plantilla,), fetchone=True)

    if not vacante:
        return JSONResponse(status_code=404, content={"error": "Puesto no encontrado"})
    if vacante["empleados_activos"] >= vacante["plantilla_autorizada"]:
        return JSONResponse(status_code=400, content={"error": "No hay vacantes disponibles"})

    # ✅ Insertar nuevo empleado
    query_insert = """
        INSERT INTO ws_rh_Empleados 
            (id_plantilla, curp, nss, rfc, nombre_completo, apellido_paterno, apellido_materno, fecha_nacimiento, fecha_alta, salario_diario, tipo_empleado)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, GETDATE(), ?, ?)
    """
    ejecutar_consulta_sql(
        query_insert,
        params=(id_plantilla, curp, nss, rfc, nombre_completo, apellido_paterno, apellido_materno, fecha_nacimiento, salario_diario, tipo_empleado),
        commit=True
    )

    # ✅ Actualizar plantilla
    query_update = """
        UPDATE ws_rh_PuestosPlantilla
        SET empleados_activos = empleados_activos + 1
        WHERE id_plantilla = ?
    """
    ejecutar_consulta_sql(query_update, params=(id_plantilla,), commit=True)

    return JSONResponse(content={"mensaje": "Empleado registrado correctamente"})


# -------------------- BAJA DE EMPLEADO --------------------
@router.post("/baja_empleado", response_class=JSONResponse)
def baja_empleado(
    id_empleado: int = Form(...),
    motivo: str = Form(...),
    observaciones: str = Form(None),
    access_token: str = Cookie(None)
):
    if not access_token:
        return JSONResponse(status_code=401, content={"error": "No autorizado"})

    token = access_token.replace("Bearer ", "")
    payload = verificar_access_token(token)
    if not payload or payload.get("K_Area") != 20:
        return JSONResponse(status_code=403, content={"error": "Acceso denegado"})

    # ✅ Obtener plantilla asociada
    query_get = """
        SELECT id_plantilla FROM ws_rh_Empleados WHERE id_empleado = ? AND activo = 1
    """
    empleado = ejecutar_consulta_sql(query_get, params=(id_empleado,), fetchone=True)
    if not empleado:
        return JSONResponse(status_code=404, content={"error": "Empleado no encontrado o ya inactivo"})

    id_plantilla = empleado["id_plantilla"]

    # ✅ Marcar empleado como inactivo
    ejecutar_consulta_sql("UPDATE ws_rh_Empleados SET activo = 0 WHERE id_empleado = ?", params=(id_empleado,), commit=True)

    # ✅ Registrar baja
    ejecutar_consulta_sql(
        "INSERT INTO ws_rh_Bajas (id_empleado, motivo, observaciones) VALUES (?, ?, ?)",
        params=(id_empleado, motivo, observaciones),
        commit=True
    )

    # ✅ Actualizar plantilla (liberar vacante)
    ejecutar_consulta_sql(
        "UPDATE ws_rh_PuestosPlantilla SET empleados_activos = empleados_activos - 1 WHERE id_plantilla = ?",
        params=(id_plantilla,),
        commit=True
    )

    return JSONResponse(content={"mensaje": "Empleado dado de baja correctamente"})

# -------------------- OBTENER LISTA DE EMPLEADOS ACTIVOS (CORREGIDO) --------------------
@router.get("/empleados", response_class=JSONResponse)
def obtener_empleados_activos(access_token: str = Cookie(None)):
    if not access_token:
        return JSONResponse(status_code=401, content={"error": "No autorizado"})

    token = access_token.replace("Bearer ", "")
    payload = verificar_access_token(token)
    if not payload or payload.get("K_Area") != 20:
        return JSONResponse(status_code=403, content={"error": "Acceso denegado"})

    query = """
        SELECT
            e.id_empleado,
            e.numero_empleado,
            e.nombre_completo,
            e.apellido_paterno,
            e.apellido_materno,
            e.curp,
            e.nss,
            e.rfc,
            e.fecha_alta,
            p.nombre_puesto,
            p.departamento,
            planta.nombre_planta
        FROM ws_rh_Empleados AS e
        INNER JOIN ws_rh_PuestosPlantilla AS p ON p.id_plantilla = e.id_plantilla
        INNER JOIN ws_rh_EmpresasPlantas AS planta ON planta.id_planta = p.id_planta
        WHERE e.activo = 1
        ORDER BY planta.nombre_planta, p.departamento, e.nombre_completo
    """
    empleados = ejecutar_consulta_sql(query, fetchall=True)
    
    # 💥 CONVERSIÓN DE FECHA A STRING (SOLUCIÓN)
    for empleado in empleados:
        if 'fecha_alta' in empleado and empleado['fecha_alta']:
            # Convertir el objeto date/datetime a string en formato YYYY-MM-DD
            empleado['fecha_alta'] = empleado['fecha_alta'].strftime('%Y-%m-%d')

    return JSONResponse(content=empleados)


# -------------------- EDITAR EMPLEADO --------------------
@router.post("/editar_empleado", response_class=JSONResponse)
def editar_empleado(
    id_empleado: int = Form(...),
    nombre_completo: str = Form(...),
    apellido_paterno: str = Form(...),
    apellido_materno: str = Form(None),
    curp: str = Form(...),
    nss: str = Form(...),
    rfc: str = Form(...),
    fecha_nacimiento: str = Form(...),
    id_plantilla_nueva: int = Form(...),
    salario_diario: float = Form(...),
    tipo_empleado: str = Form(...),
    access_token: str = Cookie(None)
):
    # ... (Verificación de seguridad omitida)

    # 1. Obtener el id_plantilla actual y verificar si hay cambio
    query_current = "SELECT id_plantilla FROM ws_rh_Empleados WHERE id_empleado = ?"
    empleado_actual = ejecutar_consulta_sql(query_current, params=(id_empleado,), fetchone=True)

    if not empleado_actual:
        return JSONResponse(status_code=404, content={"error": "Empleado no encontrado"})

    id_plantilla_actual = empleado_actual["id_plantilla"]

    # 2. Si el puesto (id_plantilla) es diferente, ajustamos los contadores
    if id_plantilla_nueva != id_plantilla_actual:
        
        # A. Verificar si hay vacante disponible en el nuevo puesto
        query_check = "SELECT plantilla_autorizada, empleados_activos FROM ws_rh_PuestosPlantilla WHERE id_plantilla = ?"
        vacante_nueva = ejecutar_consulta_sql(query_check, params=(id_plantilla_nueva,), fetchone=True)
        
        if not vacante_nueva:
             return JSONResponse(status_code=404, content={"error": "Puesto destino no encontrado"})

        if vacante_nueva["empleados_activos"] >= vacante_nueva["plantilla_autorizada"]:
            return JSONResponse(status_code=400, content={"error": "No hay vacantes disponibles en el nuevo puesto"})

        # B. Liberar vacante del puesto actual
        ejecutar_consulta_sql(
            "UPDATE ws_rh_PuestosPlantilla SET empleados_activos = empleados_activos - 1 WHERE id_plantilla = ?",
            params=(id_plantilla_actual,),
            commit=True
        )

        # C. Ocupar vacante en el nuevo puesto
        ejecutar_consulta_sql(
            "UPDATE ws_rh_PuestosPlantilla SET empleados_activos = empleados_activos + 1 WHERE id_plantilla = ?",
            params=(id_plantilla_nueva,),
            commit=True
        )

    # 3. Actualizar los datos del empleado
    query_update = """
        UPDATE ws_rh_Empleados SET
            id_plantilla = ?,
            curp = ?,
            nss = ?,
            rfc = ?,
            nombre_completo = ?,
            apellido_paterno = ?,
            apellido_materno = ?,
            fecha_nacimiento = ?,
            salario_diario = ?,
            tipo_empleado = ?
        WHERE id_empleado = ?
    """
    ejecutar_consulta_sql(
        query_update,
        params=(
            id_plantilla_nueva, curp.upper(), nss, rfc.upper(), nombre_completo, apellido_paterno, apellido_materno,
            fecha_nacimiento, salario_diario, tipo_empleado, id_empleado
        ),
        commit=True
    )

    return JSONResponse(content={"mensaje": "Datos del empleado actualizados correctamente"})

# rh.py (agregar esta función)

# -------------------- OBTENER KPIS DE PLANTILLA --------------------
@router.get("/kpis_plantilla", response_class=JSONResponse)
def obtener_kpis_plantilla(access_token: str = Cookie(None)):
    # ... (Verificación de seguridad omitida)
    
    query = """
        SELECT
            SUM(plantilla_autorizada) AS total_plantilla,
            SUM(empleados_activos) AS total_activos,
            SUM(vacantes_disponibles) AS total_vacantes
        FROM ws_rh_PuestosPlantilla
    """
    kpis = ejecutar_consulta_sql(query, fetchone=True)
    
    if not kpis:
        kpis = {"total_plantilla": 0, "total_activos": 0, "total_vacantes": 0}
        
    # Calcular Tasa de Ocupación
    if kpis["total_plantilla"] > 0:
        kpis["tasa_ocupacion"] = round((kpis["total_activos"] / kpis["total_plantilla"]) * 100, 2)
    else:
        kpis["tasa_ocupacion"] = 0

    return JSONResponse(content=kpis)


# rh.py (nueva función)

# -------------------- OBTENER LISTA DE PLANTAS --------------------
@router.get("/plantas", response_class=JSONResponse)
def obtener_plantas(access_token: str = Cookie(None)):
    # Asumo que la verificación de seguridad ya está implementada
    # ... (Verificación de seguridad)

    query = """
        SELECT 
            id_planta, 
            nombre_planta 
        FROM ws_rh_EmpresasPlantas 
        ORDER BY nombre_planta
    """
    plantas = ejecutar_consulta_sql(query, fetchall=True)
    return JSONResponse(content=plantas)

# NOTA: Tu ruta /vacantes_disponibles se mantendrá igual, ya que es general.