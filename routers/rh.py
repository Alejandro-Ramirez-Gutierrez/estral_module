from fastapi import APIRouter, Request, Cookie, Form
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from utils.auth import verificar_access_token
from services.db_service import ejecutar_consulta_sql
from datetime import date, datetime
from decimal import Decimal

router = APIRouter()
templates = Jinja2Templates(directory="templates")
templates.env.globals["datetime"] = datetime


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


# -------------------- OBTENER VACANTES DISPONIBLES --------------------
@router.get("/vacantes_disponibles", response_class=JSONResponse)
def obtener_vacantes_disponibles(access_token: str = Cookie(None)):

    query = """
        SELECT 
            p.id_plantilla,
            e.id_planta,   
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

@router.get("/vacantes_organizadas", response_class=JSONResponse)
def vacantes_organizadas(access_token: str = Cookie(None)):
    if not access_token:
        return JSONResponse(status_code=401, content={"error": "No autorizado"})
    
    token = access_token.replace("Bearer ", "")
    payload = verificar_access_token(token)
    if not payload or payload.get("K_Area") != 20:
        return JSONResponse(status_code=403, content={"error": "Acceso denegado"})

    # ✅ Consulta todos los puestos con vacantes
    query = """
        SELECT 
            e.id_planta,
            e.nombre_planta,
            p.departamento,
            p.id_plantilla,
            p.nombre_puesto,
            p.plantilla_autorizada,
            p.empleados_activos,
            p.vacantes_disponibles
        FROM ws_rh_PuestosPlantilla AS p
        INNER JOIN ws_rh_EmpresasPlantas AS e ON e.id_planta = p.id_planta
        WHERE p.vacantes_disponibles > 0
        ORDER BY e.nombre_planta, p.departamento, p.nombre_puesto
    """
    resultados = ejecutar_consulta_sql(query, fetchall=True)

    # ✅ Organizar por planta -> departamento -> puestos
    plantas = {}
    for r in resultados:
        planta_id = r["id_planta"]
        depto = r["departamento"]
        if planta_id not in plantas:
            plantas[planta_id] = {
                "id_planta": planta_id,
                "nombre_planta": r["nombre_planta"],
                "total_vacantes": 0,
                "departamentos": {}
            }
        if depto not in plantas[planta_id]["departamentos"]:
            plantas[planta_id]["departamentos"][depto] = {
                "nombre_departamento": depto,
                "total_vacantes": 0,
                "puestos": []
            }
        
        # Agregamos el puesto
        plantas[planta_id]["departamentos"][depto]["puestos"].append({
            "id_plantilla": r["id_plantilla"],
            "nombre_puesto": r["nombre_puesto"],
            "autorizada": r["plantilla_autorizada"],
            "empleados_activos": r["empleados_activos"],
            "vacantes": r["vacantes_disponibles"]
        })
        
        # Suma vacantes
        plantas[planta_id]["departamentos"][depto]["total_vacantes"] += r["vacantes_disponibles"]
        plantas[planta_id]["total_vacantes"] += r["vacantes_disponibles"]

    # Convertimos dict a lista para frontend
    plantas_list = []
    for p in plantas.values():
        p["departamentos"] = list(p["departamentos"].values())
        plantas_list.append(p)

    return JSONResponse(content=plantas_list)


# -------------------- REGISTRAR EMPLEADO --------------------
@router.post("/registrar_empleado", response_class=JSONResponse)
def registrar_empleado(
    # Campos ya existentes
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
    fecha_alta: str = Form(...),
    numero_empleado: str = Form(None),
    estado_civil: str = Form(None),
    sexo: str = Form(None),
    telefono_movil: str = Form(None),
    calle: str = Form(None),
    cp: str = Form(None),
    municipio: str = Form(None),
    colonia: str = Form(None),
    contacto_emergencia: str = Form(None),
    parentesco_emergencia: str = Form(None),
    tel_emergencia: str = Form(None),
    email_corp: str = Form(None),
    tipo_relacion_laboral: str = Form(None),
    escolaridad: str = Form(None),
    
    access_token: str = Cookie(None)
):
    # 1. Validación de Token
    if not access_token:
        return JSONResponse(status_code=401, content={"error": "No autorizado"})

    token = access_token.replace("Bearer ", "")
    payload = verificar_access_token(token)
    if not payload or payload.get("K_Area") != 20:
        return JSONResponse(status_code=403, content={"error": "Acceso denegado"})

    # 2. Verificar si hay vacantes disponibles 
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

    query_insert = """
        INSERT INTO ws_rh_Empleados (
            id_plantilla, curp, nss, rfc, nombre_completo, apellido_paterno, apellido_materno, 
            fecha_nacimiento, fecha_alta, salario_diario, tipo_empleado, activo,
            numero_empleado, estado_civil, sexo, telefono_movil, calle, cp, municipio, colonia, 
            contacto_emergencia, parentesco_emergencia, tel_emergencia, email_corp, tipo_relacion_laboral, escolaridad
        )
        VALUES (
            ?, ?, ?, ?, ?, ?, ?, 
            ?, ?, ?, ?, 1,
            ?, ?, ?, ?, ?, ?, ?, ?, 
            ?, ?, ?, ?, ?, ?
        )
    """
    params_insert = (
    id_plantilla, curp, nss, rfc, nombre_completo, apellido_paterno, apellido_materno, 
    fecha_nacimiento, 
    fecha_alta, 
    salario_diario, tipo_empleado,
    numero_empleado, estado_civil, sexo, telefono_movil, calle, cp, municipio, colonia, 
    contacto_emergencia, parentesco_emergencia, tel_emergencia, email_corp, tipo_relacion_laboral, escolaridad
)
    
    try:
        # Ejecutamos la inserción primero
        ejecutar_consulta_sql(query_insert, params=params_insert, commit=True)

        # 4. ✅ Actualizar plantilla (SOLO SI LA INSERCIÓN FUE EXITOSA)
        query_update = """
            UPDATE ws_rh_PuestosPlantilla
            SET empleados_activos = empleados_activos + 1
            WHERE id_plantilla = ?
        """
        ejecutar_consulta_sql(query_update, params=(id_plantilla,), commit=True)
        
        return JSONResponse(content={"mensaje": "Empleado registrado correctamente"})

    except Exception as e:
        # Esto captura el error de SQL (como el 23000 que indica clave duplicada)
        error_str = str(e)
        
        # Intentamos identificar si es una violación de clave única
        if 'Violation of UNIQUE KEY constraint' in error_str or 'duplicate key' in error_str:
            
            # Puedes ser más específico si quieres nombrar la columna:
            if 'UQ__ws_rh_Em__55BF368CFC0015D3' in error_str:
                # El constraint que te falló con NULL
                mensaje = "Error: El campo (probablemente Número de Empleado) ya existe o tiene un valor NULO duplicado. Asegura un valor único."
            elif 'CURP' in error_str:
                 mensaje = "Error: La CURP ya está registrada en el sistema."
            elif 'RFC' in error_str:
                 mensaje = "Error: El RFC ya está registrado en el sistema."
            else:
                 mensaje = "Error: Violación de clave única. El CURP, RFC o Número de Empleado ya existe."
            
            return JSONResponse(
                status_code=400, 
                content={"error": mensaje}
            )
        
        # Para cualquier otro error
        print(f"ERROR Desconocido en el registro: {error_str}") 
        return JSONResponse(
            status_code=500, 
            content={"error": "Error interno del servidor al registrar."}
        )


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

    # ✅ Actualizar plantilla
    ejecutar_consulta_sql(
        "UPDATE ws_rh_PuestosPlantilla SET empleados_activos = empleados_activos - 1 WHERE id_plantilla = ?",
        params=(id_plantilla,),
        commit=True
    )

    return JSONResponse(content={"mensaje": "Empleado dado de baja correctamente"})


# -------------------- OBTENER LISTA DE EMPLEADOS ACTIVOS --------------------
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
    
    
    for empleado in empleados:
        if 'fecha_alta' in empleado and empleado['fecha_alta']:
            # Convertir el objeto date/datetime a string en formato YYYY-MM-DD
            empleado['fecha_alta'] = empleado['fecha_alta'].strftime('%Y-%m-%d')

    return JSONResponse(content=empleados)


# -------------------- EDITAR EMPLEADO (VERSION COMPLETA) --------------------
@router.post("/editar_empleado", response_class=JSONResponse)
def editar_empleado(
    # CAMPOS ORIGINALES
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
    numero_empleado: str = Form(None),
    estado_civil: str = Form(None),
    sexo: str = Form(None),
    telefono_movil: str = Form(None),
    email_corp: str = Form(None),
    escolaridad: str = Form(None),
    tipo_relacion_laboral: str = Form(None),
    calle: str = Form(None),
    colonia: str = Form(None),
    municipio: str = Form(None),
    cp: str = Form(None),
    contacto_emergencia: str = Form(None),
    parentesco_emergencia: str = Form(None),
    tel_emergencia: str = Form(None),
    
    access_token: str = Cookie(None)
):

    # 1. Obtener el id_plantilla actual y verificar si hay cambio
    query_current = "SELECT id_plantilla FROM ws_rh_Empleados WHERE id_empleado = ?"
    empleado_actual = ejecutar_consulta_sql(query_current, params=(id_empleado,), fetchone=True)

    if not empleado_actual:
        return JSONResponse(status_code=404, content={"error": "Empleado no encontrado"})

    id_plantilla_actual = empleado_actual["id_plantilla"]

    # 2. LÓGICA DE ACTUALIZACIÓN DE CONTADORES DE PLANTILLA
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
            tipo_empleado = ?,
            
            numero_empleado = ?,
            estado_civil = ?,
            sexo = ?,
            telefono_movil = ?,
            email_corp = ?,
            escolaridad = ?,
            tipo_relacion_laboral = ?,
            calle = ?,
            colonia = ?,
            municipio = ?,
            cp = ?,
            contacto_emergencia = ?,
            parentesco_emergencia = ?,
            tel_emergencia = ?
        WHERE id_empleado = ?
    """
    ejecutar_consulta_sql(
        query_update,
        params=(
            # Parámetros Antiguos
            id_plantilla_nueva, curp.upper(), nss, rfc.upper(), nombre_completo, apellido_paterno, 
            apellido_materno, fecha_nacimiento, salario_diario, tipo_empleado,
            
            #  Parámetros Nuevos 
            numero_empleado, estado_civil, sexo, telefono_movil, email_corp, escolaridad, 
            tipo_relacion_laboral, calle, colonia, municipio, cp, contacto_emergencia, 
            parentesco_emergencia, tel_emergencia,
            
            # WHERE
            id_empleado
        ),
        commit=True
    )

    return JSONResponse(content={"mensaje": "Datos del empleado actualizados correctamente"})


# -------------------- OBTENER KPIS DE PLANTILLA --------------------
@router.get("/kpis_plantilla", response_class=JSONResponse)
def obtener_kpis_plantilla(access_token: str = Cookie(None)):
    
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


# -------------------- OBTENER LISTA DE PLANTAS --------------------
@router.get("/plantas", response_class=JSONResponse)
def obtener_plantas(access_token: str = Cookie(None)):
    
    query = """
        SELECT 
            id_planta, 
            nombre_planta 
        FROM ws_rh_EmpresasPlantas 
        ORDER BY nombre_planta
    """
    plantas = ejecutar_consulta_sql(query, fetchall=True)
    return JSONResponse(content=plantas)


# -------------------- BUSCAR EMPLEADO DADO DE BAJA --------------------
@router.get("/buscar_empleado_baja/{numero_empleado}", response_class=JSONResponse)
def buscar_empleado_baja(
    numero_empleado: str,
    access_token: str = Cookie(None)
):
    # 1. Buscar al empleado por numero_empleado
    query_empleado = """
        SELECT * FROM ws_rh_Empleados 
        WHERE numero_empleado = ? AND activo = 0
    """
    empleado = ejecutar_consulta_sql(query_empleado, params=(numero_empleado,), fetchone=True)

    if not empleado:
        return JSONResponse(status_code=404, content={"error": "No se encontró un empleado inactivo con ese número."})
    
    id_empleado = empleado["id_empleado"]

    # 2. Buscar el último registro de baja (el más reciente)
    query_baja = """
        SELECT TOP 1 motivo, observaciones 
        FROM ws_rh_Bajas 
        WHERE id_empleado = ? 
        ORDER BY fecha_baja DESC 
    """
    baja = ejecutar_consulta_sql(query_baja, params=(id_empleado,), fetchone=True)
    
    if not baja:
        baja = {"motivo": "Motivo no registrado", "observaciones": "Sin observaciones de baja."}


    # 3. Devolver los datos del empleado y el motivo de baja
    empleado_data = {}
    # Conversión de fechas (date/datetime) Y números decimales
    for key, value in empleado.items():
        if isinstance(value, (date, datetime)):
            empleado_data[key] = value.isoformat()
        elif isinstance(value, Decimal):
            empleado_data[key] = float(value) 
        else:
            empleado_data[key] = value
    
    return JSONResponse(content={
        "empleado": empleado_data,
        "baja": baja
    })

# -------------------- PROCESAR REINGRESO DE EMPLEADO --------------------
@router.post("/reingreso_empleado", response_class=JSONResponse)
def reingreso_empleado(
    id_empleado: int = Form(...),
    id_plantilla_anterior: int = Form(...),
    id_plantilla_nueva: int = Form(...),
    fecha_alta: str = Form(...),
    salario_diario: float = Form(...),
    telefono_movil: str = Form(None),
    access_token: str = Cookie(None)
):

    query_check = "SELECT plantilla_autorizada, empleados_activos FROM ws_rh_PuestosPlantilla WHERE id_plantilla = ?"
    vacante_nueva = ejecutar_consulta_sql(query_check, params=(id_plantilla_nueva,), fetchone=True)
    
    if not vacante_nueva:
        return JSONResponse(status_code=404, content={"error": "Puesto destino no encontrado"})

    if vacante_nueva["empleados_activos"] >= vacante_nueva["plantilla_autorizada"]:
        return JSONResponse(status_code=400, content={"error": "No hay vacantes disponibles en el nuevo puesto."})

    # 2. Lógica de Actualización de Plantilla
    if id_plantilla_nueva != id_plantilla_anterior:
        # 2a. Liberar vacante de la plantilla
        ejecutar_consulta_sql(
            "UPDATE ws_rh_PuestosPlantilla SET empleados_activos = empleados_activos - 1 WHERE id_plantilla = ?",
            params=(id_plantilla_anterior,),
            commit=True
        )
        
        # 2b. Ocupar vacante en la plantilla
        ejecutar_consulta_sql(
            "UPDATE ws_rh_PuestosPlantilla SET empleados_activos = empleados_activos + 1 WHERE id_plantilla = ?",
            params=(id_plantilla_nueva,),
            commit=True
        )
    else:
        # Si es la misma plantilla, solo sumar 1 
        ejecutar_consulta_sql(
            "UPDATE ws_rh_PuestosPlantilla SET empleados_activos = empleados_activos + 1 WHERE id_plantilla = ?",
            params=(id_plantilla_nueva,),
            commit=True
        )


    # 3. Reestablecer el registro del empleado
    query_update = """
        UPDATE ws_rh_Empleados SET
            activo = 1,                 
            id_plantilla = ?,
            fecha_alta = ?,
            salario_diario = ?,
            telefono_movil = ? 
        WHERE id_empleado = ?
    """
    ejecutar_consulta_sql(
        query_update,
        params=(
            id_plantilla_nueva, 
            fecha_alta, 
            salario_diario, 
            telefono_movil, 
            id_empleado
        ),
        commit=True
    )

    return JSONResponse(content={"mensaje": "Empleado reingresado y puesto actualizado."})


from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, Image
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet
import io
from fastapi.responses import StreamingResponse


@router.get("/generar_kardex_pdf/{numero_empleado}")
def generar_kardex_pdf(numero_empleado: int, access_token: str = Cookie(None)):
    # Verificación de token
    if not access_token:
        return JSONResponse(status_code=401, content={"error": "No autorizado"})

    token = access_token.replace("Bearer ", "")
    payload = verificar_access_token(token)
    if not payload or payload.get("K_Area") != 20:
        return JSONResponse(status_code=403, content={"error": "Acceso denegado"})

    # Obtener datos del empleado
    query = """
        SELECT e.*, p.nombre_puesto, p.departamento, planta.nombre_planta
        FROM ws_rh_Empleados AS e
        INNER JOIN ws_rh_PuestosPlantilla AS p ON e.id_plantilla = p.id_plantilla
        INNER JOIN ws_rh_EmpresasPlantas AS planta ON planta.id_planta = p.id_planta
        WHERE e.numero_empleado = ?
    """
    empleado = ejecutar_consulta_sql(query, params=(numero_empleado,), fetchone=True)

    if not empleado:
        return JSONResponse(status_code=404, content={"error": "Empleado no encontrado"})

    # Crear PDF en memoria
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=letter)
    styles = getSampleStyleSheet()
    elementos = []

    # Logo
    logo_path = "static/img/logo_kardex.png"
    try:
        logo = Image(logo_path, width=100, height=40)
        elementos.append(logo)
    except Exception:
        elementos.append(Paragraph("Logo no disponible", styles["Normal"]))
    elementos.append(Spacer(1, 12))

    # Título
    elementos.append(Paragraph("<b>KARDEX DEL EMPLEADO</b>", styles["Title"]))
    elementos.append(Spacer(1, 12))

    # Tabla de datos personales
    datos_personales = [
        ["Nombre completo", empleado["nombre_completo"]],
        ["CURP", empleado["curp"]],
        ["RFC", empleado["rfc"]],
        ["NSS", empleado["nss"]],
        ["Estado civil", empleado.get("estado_civil", "")],
        ["Sexo", empleado.get("sexo", "")],
        ["Fecha de nacimiento", str(empleado["fecha_nacimiento"]) if empleado["fecha_nacimiento"] else ""],
        ["Teléfono móvil", empleado.get("telefono_movil", "")],
        ["Domicilio", f"{empleado.get('calle', '')}, {empleado.get('colonia', '')}, CP {empleado.get('cp', '')}"],
        ["Municipio", empleado.get("municipio", "")],
    ]

    tabla_personal = Table(datos_personales, colWidths=[150, 350])
    tabla_personal.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.lightgrey),
        ('BOX', (0, 0), (-1, -1), 1, colors.black),
        ('INNERGRID', (0, 0), (-1, -1), 0.5, colors.grey),
    ]))
    elementos.append(Paragraph("<b>Datos personales</b>", styles["Heading2"]))
    elementos.append(tabla_personal)
    elementos.append(Spacer(1, 12))

    # Datos laborales
    datos_laborales = [
        ["Planta", empleado["nombre_planta"]],
        ["Departamento", empleado["departamento"]],
        ["Puesto", empleado["nombre_puesto"]],
        ["Fecha de alta", str(empleado["fecha_alta"]) if empleado["fecha_alta"] else ""],
        ["Tipo empleado", empleado["tipo_empleado"]],
        ["Tipo relación laboral", empleado.get("tipo_relacion_laboral", "")],
        ["Escolaridad", empleado.get("escolaridad", "")],
        ["Salario diario", f"${empleado['salario_diario']:.2f}" if empleado["salario_diario"] else ""],
    ]

    tabla_laboral = Table(datos_laborales, colWidths=[150, 350])
    tabla_laboral.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.lightgrey),
        ('BOX', (0, 0), (-1, -1), 1, colors.black),
        ('INNERGRID', (0, 0), (-1, -1), 0.5, colors.grey),
    ]))
    elementos.append(Paragraph("<b>Datos laborales</b>", styles["Heading2"]))
    elementos.append(tabla_laboral)

    # Generar el PDF
    doc.build(elementos)
    buffer.seek(0)

    return StreamingResponse(buffer, media_type="application/pdf",
        headers={"Content-Disposition": f"inline; filename=Kardex_{empleado['nombre_completo']}.pdf"})
