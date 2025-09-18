
# Comando para encender entorno virtual: source venv/bin/activate
# levantar servidor ejecturar: uvicorn main:app --reload
# estral_modulo/main.py
from fastapi import FastAPI, Request, Form, Cookie
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from routers import auth_requisiciones
from utils.auth import crear_access_token, verificar_access_token
from fastapi import Body
from fastapi.staticfiles import StaticFiles
from services.db_service import (
    valida_usuario,
    login_user,
    obtener_ordenes_para_autorizar,
    cancelar_orden_compra,
    obtener_motivos_cancelacion,
    autorizar_orden 
)

app = FastAPI(title="Estral Módulo - Autorización Requisiciones")
templates = Jinja2Templates(directory="templates")
app.mount("/static", StaticFiles(directory="static"), name="static")
app.include_router(auth_requisiciones.router, prefix="/auth", tags=["Autenticación"])

# ------------------- LOGIN -------------------
@app.get("/", response_class=HTMLResponse)
def get_login(request: Request):
    return templates.TemplateResponse("login.html", {"request": request})

@app.post("/login", response_class=HTMLResponse)
def post_login(request: Request, login: str = Form(...), contrasenia: str = Form(...)):
    resultado = login_user(login, contrasenia)
    if "user" in resultado:
        user = resultado["user"]
        token_data = {
            "sub": login,
            "K_Empleado": user["K_Empleado"],
            "D_Empleado": user["D_Empleado"]
        }
        token = crear_access_token(token_data)
        response = RedirectResponse(url="/dashboard", status_code=303)
        response.set_cookie(key="access_token", value=f"Bearer {token}", httponly=True)
        return response

    return templates.TemplateResponse(
        "login.html",
        {"request": request, "error": resultado.get("error", "Usuario o contraseña incorrectos")}
    )

# ------------------- DASHBOARD -------------------
@app.get("/dashboard", response_class=HTMLResponse)
def dashboard(request: Request, access_token: str = Cookie(None)):
    if not access_token:
        return RedirectResponse(url="/", status_code=303)

    token = access_token.replace("Bearer ", "")
    payload = verificar_access_token(token)
    if not payload:
        return RedirectResponse(url="/", status_code=303)

    usuario = payload.get("sub", "Usuario")
    k_empleado = payload.get("K_Empleado")
    ordenes = obtener_ordenes_para_autorizar(k_empleado) if k_empleado else []

    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "usuario": usuario,
        "K_Empleado": k_empleado,
        "ordenes": ordenes
    })

# ------------------- MOTIVOS CANCELACIÓN -------------------
@app.get("/dashboard/motivos_cancelacion")
def api_motivos_cancelacion(access_token: str = Cookie(None)):
    if not access_token:
        return JSONResponse(status_code=401, content={"error": "No autorizado"})

    token = access_token.replace("Bearer ", "")
    payload = verificar_access_token(token)
    if not payload:
        return JSONResponse(status_code=401, content={"error": "No autorizado"})

    motivos = obtener_motivos_cancelacion()
    return {"motivos": motivos}

# ------------------- CANCELAR ORDEN -------------------
@app.post("/dashboard/cancelar_orden")
def api_cancelar_orden(
    k_orden_compra: int = Form(...),
    k_motivo: int = Form(1),  # valor por defecto para seguridad
    access_token: str = Cookie(None)
):
    if not access_token:
        return JSONResponse(status_code=401, content={"error": "No autorizado"})

    token = access_token.replace("Bearer ", "")
    payload = verificar_access_token(token)
    if not payload:
        return JSONResponse(status_code=401, content={"error": "Token inválido"})

    k_empleado = payload.get("K_Empleado")
    if not k_empleado:
        return JSONResponse(status_code=400, content={"error": "No se encontró el número de empleado"})

    # Llamamos al service que ejecuta el procedure para cancelar
    result = cancelar_orden_compra(k_orden_compra, k_empleado, k_motivo)
    if "error" in result:
        return JSONResponse(status_code=400, content=result)

    # Una vez cancelada, volvemos a traer las órdenes para actualizar la tabla
    ordenes = obtener_ordenes_para_autorizar(k_empleado)
    return {"success": True, "ordenes": ordenes}

from fastapi import Body

@app.post("/dashboard/autorizar_orden")
def api_autorizar_orden(
    k_orden_compra: int = Body(..., embed=True),
    access_token: str = Cookie(None)
):
    if not access_token:
        return JSONResponse(status_code=401, content={"error": "No autorizado"})

    token = access_token.replace("Bearer ", "")
    payload = verificar_access_token(token)
    if not payload:
        return JSONResponse(status_code=401, content={"error": "Token inválido"})

    k_empleado = payload.get("K_Empleado")
    if not k_empleado:
        return JSONResponse(status_code=400, content={"error": "No se encontró el número de empleado"})

    try:
        b_notificacion, mensaje = autorizar_orden(k_orden_compra, k_empleado)

        # Recargamos las ordenes para actualizar la tabla
        ordenes = obtener_ordenes_para_autorizar(k_empleado)

        return {
            "success": mensaje == "",
            "Mensaje": mensaje,
            "B_Notificacion": b_notificacion,
            "ordenes": ordenes
        }

    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})