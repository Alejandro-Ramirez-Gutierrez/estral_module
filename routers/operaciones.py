# routers/operaciones.py
from fastapi import APIRouter, Request, Cookie, Query, Form
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from utils.auth import verificar_access_token
from services.db_service import ejecutar_consulta_sql
from datetime import date, datetime

router = APIRouter()
templates = Jinja2Templates(directory="templates")
templates.env.globals["datetime"] = datetime

# =================== LISTAS DE PERMISOS ===================
USUARIOS_SOLICITUDES = [8811, 8870]   # Pueden crear solicitudes 
USUARIOS_APROBADORES = [8740, 5]       # Pueden aprobar o rechazar
USUARIOS_VISUALIZADORES = [8811, 8661, 8870, 8740, 4, 5]  # Pueden entrar y ver

# =================== HELPERS ===================
def validar_token_operaciones(access_token: str):
    """Verifica el token y devuelve el payload si es válido."""
    if not access_token:
        return None
    token = access_token.replace("Bearer ", "")
    payload = verificar_access_token(token)
    return payload if payload else None

def agregar_usuario(lista: list, id_usuario: int):
    """Agrega un usuario a una lista si no está."""
    if id_usuario not in lista:
        lista.append(id_usuario)

def quitar_usuario(lista: list, id_usuario: int):
    """Quita un usuario de una lista si está."""
    if id_usuario in lista:
        lista.remove(id_usuario)

# =================== RUTAS PRINCIPALES ===================
@router.get("/", response_class=HTMLResponse)
def dashboard_operaciones(request: Request, access_token: str = Cookie(None)):
    """Vista principal del módulo de Operaciones."""
    payload = validar_token_operaciones(access_token)
    if not payload:
        return JSONResponse(status_code=401, content={"error": "No autorizado"})

    k_empleado = payload.get("K_Empleado")

    if k_empleado not in USUARIOS_VISUALIZADORES:
        return JSONResponse(status_code=403, content={"error": "Sin permisos para acceder al módulo."})

    return templates.TemplateResponse("operaciones.html", {
        "request": request,
        "usuario": payload.get("D_Empleado", "Usuario"),
        "k_empleado": k_empleado
    })


# ======================== INCIDENCIAS ======================
@router.get("/incidencias", response_class=HTMLResponse)
def incidencias(request: Request, access_token: str = Cookie(None)):
    payload = validar_token_operaciones(access_token)
    if not payload:
        return JSONResponse(status_code=401, content={"error": "No autorizado"})

    if payload["K_Empleado"] not in USUARIOS_VISUALIZADORES:
        return JSONResponse(status_code=403, content={"error": "Sin permisos."})

    return templates.TemplateResponse("incidencias.html", {"request": request, "usuario": payload.get("D_Empleado")})


@router.post("/incidencias/nueva")
def nueva_incidencia(
    request: Request,
    empleado_id: int = Form(...),
    tipo_incidencia: str = Form(...),
    fecha_incidencia: date = Form(...),
    motivo: str = Form(None),
    observaciones: str = Form(None),
    access_token: str = Cookie(None)
):
    payload = validar_token_operaciones(access_token)
    if not payload:
        return JSONResponse(status_code=401, content={"error": "No autorizado"})

    if payload["K_Empleado"] not in USUARIOS_SOLICITUDES:
        return JSONResponse(status_code=403, content={"error": "No tienes permiso para registrar incidencias."})

    query = """
        INSERT INTO ws_rh_Incidencias (id_empleado, tipo_incidencia, fecha_incidencia, motivo, observaciones)
        VALUES (?, ?, ?, ?, ?)
    """
    ejecutar_consulta_sql(query, params=(empleado_id, tipo_incidencia, fecha_incidencia, motivo, observaciones), commit=True)

    return JSONResponse({"ok": "Incidencia registrada correctamente."})


# ========================= VACACIONES =========================
@router.get("/vacaciones", response_class=HTMLResponse)
def vacaciones(request: Request, access_token: str = Cookie(None)):
    payload = validar_token_operaciones(access_token)
    if not payload:
        return JSONResponse(status_code=401, content={"error": "No autorizado"})

    return templates.TemplateResponse("vacaciones.html", {"request": request, "usuario": payload.get("D_Empleado")})



@router.post("/vacaciones/nueva")
def nueva_vacacion(
    request: Request,
    empleado_id: int = Form(...),
    fecha_inicio: date = Form(...),
    fecha_fin: date = Form(...),
    motivo: str = Form(None),
    access_token: str = Cookie(None)
):
    payload = validar_token_operaciones(access_token)
    if not payload:
        return JSONResponse(status_code=401, content={"error": "No autorizado"})

    if payload["K_Empleado"] not in USUARIOS_SOLICITUDES:
        return JSONResponse(status_code=403, content={"error": "No tienes permiso para solicitar vacaciones."})

    query = """
        INSERT INTO ws_rh_Vacaciones (id_empleado, fecha_inicio, fecha_fin, motivo, fecha_solicitud, estatus)
        VALUES (?, ?, ?, ?, GETDATE(), 'Pendiente')
    """
    ejecutar_consulta_sql(query, params=(empleado_id, fecha_inicio, fecha_fin, motivo), commit=True)

    return JSONResponse({"ok": "Solicitud de vacaciones enviada correctamente."})


# ================== REPORTES =================================
@router.get("/reportes", response_class=HTMLResponse)
def reportes(request: Request, access_token: str = Cookie(None)):
    payload = validar_token_operaciones(access_token)
    if not payload:
        return JSONResponse(status_code=401, content={"error": "No autorizado"})

    return templates.TemplateResponse("reportes.html", {"request": request, "usuario": payload.get("D_Empleado")})



# =================== ADMIN PERMISOS ===================
@router.get("/ver_permisos")
def ver_permisos():
    """Devuelve los usuarios en cada lista."""
    return {
        "solicitudes": USUARIOS_SOLICITUDES,
        "aprobadores": USUARIOS_APROBADORES,
        "visualizadores": USUARIOS_VISUALIZADORES
    }



@router.post("/agregar_permiso")
def agregar_permiso(tipo: str = Query(...), id_usuario: int = Query(...)):
    """Permite agregar usuarios a listas de permisos dinámicamente."""
    if tipo == "solicitud":
        agregar_usuario(USUARIOS_SOLICITUDES, id_usuario)
    elif tipo == "aprobador":
        agregar_usuario(USUARIOS_APROBADORES, id_usuario)
    elif tipo == "visualizador":
        agregar_usuario(USUARIOS_VISUALIZADORES, id_usuario)
    else:
        return JSONResponse(status_code=400, content={"error": "Tipo no válido"})

    return {"ok": f"Usuario {id_usuario} agregado a {tipo}"}



@router.post("/quitar_permiso")
def quitar_permiso(tipo: str = Query(...), id_usuario: int = Query(...)):
    """Permite remover usuarios de listas de permisos."""
    if tipo == "solicitud":
        quitar_usuario(USUARIOS_SOLICITUDES, id_usuario)
    elif tipo == "aprobador":
        quitar_usuario(USUARIOS_APROBADORES, id_usuario)
    elif tipo == "visualizador":
        quitar_usuario(USUARIOS_VISUALIZADORES, id_usuario)
    else:
        return JSONResponse(status_code=400, content={"error": "Tipo no válido"})

    return {"ok": f"Usuario {id_usuario} removido de {tipo}"}
