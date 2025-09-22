#-------------------- Encender entorno virtual : source venv/bin/activate macos
#-------------------- Levantar servidor virtual: uvicorn main:app --reload
# estral_modulo/main.py
from fastapi import FastAPI, Request, Form, Cookie, Body, Query
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from routers import auth_requisiciones
from utils.auth import crear_access_token, verificar_access_token
from services.db_service import (
    login_user,
    obtener_ordenes_para_autorizar,
    cancelar_orden_compra,
    obtener_motivos_cancelacion,
    autorizar_orden,
    ejecutar_consulta_sql
)
from datetime import datetime
from calendar import month_name

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
        token_data = {"sub": login, "K_Empleado": user["K_Empleado"], "D_Empleado": user["D_Empleado"]}
        token = crear_access_token(token_data)
        response = RedirectResponse(url="/dashboard", status_code=303)
        response.set_cookie(key="access_token", value=f"Bearer {token}", httponly=True)
        return response
    return templates.TemplateResponse("login.html", {"request": request, "error": resultado.get("error", "Usuario o contraseña incorrectos")})

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
    return templates.TemplateResponse("dashboard.html", {"request": request, "usuario": usuario, "K_Empleado": k_empleado, "ordenes": ordenes})

# ------------------- MOTIVOS CANCELACIÓN -------------------
@app.get("/dashboard/motivos_cancelacion")
def api_motivos_cancelacion(access_token: str = Cookie(None)):
    if not access_token:
        return JSONResponse(status_code=401, content={"error": "No autorizado"})
    token = access_token.replace("Bearer ", "")
    payload = verificar_access_token(token)
    if not payload:
        return JSONResponse(status_code=401, content={"error": "No autorizado"})
    return {"motivos": obtener_motivos_cancelacion()}

# ------------------- CANCELAR ORDEN -------------------
@app.post("/dashboard/cancelar_orden")
def api_cancelar_orden(k_orden_compra: int = Form(...), k_motivo: int = Form(1), access_token: str = Cookie(None)):
    if not access_token:
        return JSONResponse(status_code=401, content={"error": "No autorizado"})
    token = access_token.replace("Bearer ", "")
    payload = verificar_access_token(token)
    if not payload:
        return JSONResponse(status_code=401, content={"error": "Token inválido"})
    k_empleado = payload.get("K_Empleado")
    if not k_empleado:
        return JSONResponse(status_code=400, content={"error": "No se encontró el número de empleado"})
    result = cancelar_orden_compra(k_orden_compra, k_empleado, k_motivo)
    if "error" in result:
        return JSONResponse(status_code=400, content=result)
    ordenes = obtener_ordenes_para_autorizar(k_empleado)
    return {"success": True, "ordenes": ordenes}

# ------------------- AUTORIZAR ORDEN -------------------
@app.post("/dashboard/autorizar_orden")
def api_autorizar_orden(k_orden_compra: int = Body(..., embed=True), access_token: str = Cookie(None)):
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
        ordenes = obtener_ordenes_para_autorizar(k_empleado)
        return {"success": mensaje == "", "Mensaje": mensaje, "B_Notificacion": b_notificacion, "ordenes": ordenes}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

# ------------------- VALANCE -------------------
def validar_acceso_valance(payload):
    k_empleado = payload.get("K_Empleado")
    k_area = payload.get("K_Area")
    AREAS_PERMITIDAS = [99, 20, 22, 23]
    EMPLEADOS_PERMITIDOS = [8811, 8870, 8740,4,5]
    return (k_area in AREAS_PERMITIDAS) or (k_empleado in EMPLEADOS_PERMITIDOS)

@app.get("/valance", response_class=HTMLResponse)
def valance(request: Request, access_token: str = Cookie(None)):
    if not access_token:
        return RedirectResponse(url="/", status_code=303)
    token = access_token.replace("Bearer ", "")
    payload = verificar_access_token(token)
    if not payload or not validar_acceso_valance(payload):
        return JSONResponse(status_code=403, content={"error": "Acceso denegado"})
    usuario = payload.get("sub", "Usuario")
    k_empleado = payload.get("K_Empleado")
    today = datetime.today()
    meses = [{"value": f"{today.year}-{str(m).zfill(2)}", "name": f"{month_name[m]} {today.year}"} for m in range(1, today.month+1)]
    return templates.TemplateResponse("valance.html", {
        "request": request,
        "usuario": usuario,
        "K_Empleado": k_empleado,
        "K_Area": payload.get("K_Area"),
        "meses": meses,
        "mes_actual": f"{today.year}-{str(today.month).zfill(2)}",
        "data": {"Total_Ordenes":0,"Cerradas":0,"Sin_Autorizar":0,"Canceladas":0,"Recepcionadas_Completas":0,"Recepcionadas_Parciales":0,"Sin_Recepcion":0},
        "top_proveedores": []
    })

# ------------------- DATOS VALANCE -------------------
@app.get("/valance/datos")
def valance_datos(mes: str = Query("", description="Mes en formato YYYY-MM"), access_token: str = Cookie(None)):
    if not access_token:
        return JSONResponse(status_code=401, content={"error": "No autorizado"})
    token = access_token.replace("Bearer ", "")
    payload = verificar_access_token(token)
    if not payload or not validar_acceso_valance(payload):
        return JSONResponse(status_code=403, content={"error": "Acceso denegado"})

    if not mes:
        mes = datetime.now().strftime("%Y-%m")
    fecha_inicio = datetime.strptime(f"{mes}-01","%Y-%m-%d")
    fecha_fin = (fecha_inicio.replace(month=fecha_inicio.month+1, day=1) 
                 if fecha_inicio.month<12 else fecha_inicio.replace(year=fecha_inicio.year+1, month=1, day=1))

    # Totales por moneda y total general en pesos
    query_totales = f"""
    SELECT 
        SUM(CASE WHEN K_Tipo_Moneda = 1 THEN precio_total_orden_compra ELSE 0 END) AS total_pesos,
        SUM(CASE WHEN K_Tipo_Moneda = 2 THEN precio_total_orden_compra ELSE 0 END) AS total_dolares,
        SUM(CASE WHEN K_Tipo_Moneda = 3 THEN precio_total_orden_compra ELSE 0 END) AS total_euros,
        SUM(precio_total_orden_compra *
            CASE K_Tipo_Moneda
                WHEN 1 THEN 1
                WHEN 2 THEN 20
                WHEN 3 THEN 24
            END
        ) AS total_general_en_pesos
    FROM Ordenes_compra
    WHERE F_Generacion >= '{fecha_inicio}' AND F_Generacion < '{fecha_fin}'
      AND B_Cerrada = 1 AND (B_Cancelada IS NULL OR B_Cancelada = 0);
    """
    totales = ejecutar_consulta_sql(query_totales, fetchone=True)

    # Resumen de ordenes (igual que antes)
    query_resumen = f"""
    WITH RParciales AS (
        SELECT O.K_Orden_Compra, COUNT(1) AS Recepciones
        FROM Ordenes_compra O
        INNER JOIN Recepcion_Articulos R ON R.K_Orden_Compra = O.K_Orden_Compra
        WHERE O.F_Generacion >= '{fecha_inicio}' AND O.F_Generacion < '{fecha_fin}'
          AND O.B_Completa = 0
          AND (O.B_Cancelada IS NULL OR O.B_Cancelada = 0)
        GROUP BY O.K_Orden_Compra
    )
    SELECT COUNT(*) AS Total_Ordenes,
           SUM(CASE WHEN O.B_Cerrada = 1 AND (O.B_Cancelada IS NULL OR O.B_Cancelada = 0) THEN 1 ELSE 0 END) AS Cerradas,
           SUM(CASE WHEN (O.B_Cerrada = 0 OR O.B_Cerrada IS NULL) AND (O.B_Cancelada IS NULL OR O.B_Cancelada = 0) THEN 1 ELSE 0 END) AS Sin_Autorizar,
           SUM(CASE WHEN O.B_Cancelada = 1 THEN 1 ELSE 0 END) AS Canceladas,
           SUM(CASE WHEN O.B_Completa = 1 AND (O.B_Cancelada IS NULL OR O.B_Cancelada = 0) THEN 1 ELSE 0 END) AS Recepcionadas_Completas,
           COUNT(RP.K_Orden_Compra) AS Recepcionadas_Parciales,
           SUM(CASE WHEN O.B_Completa = 0 AND RP.K_Orden_Compra IS NULL THEN 1 ELSE 0 END) AS Sin_Recepcion
    FROM Ordenes_compra O
    LEFT JOIN RParciales RP ON RP.K_Orden_Compra = O.K_Orden_Compra
    WHERE O.F_Generacion >= '{fecha_inicio}' AND O.F_Generacion < '{fecha_fin}';
    """
    resumen = ejecutar_consulta_sql(query_resumen, fetchone=True)

    # Top proveedores en pesos
    query_top_prov = f"""
    SELECT P.D_Proveedor,
           SUM(O.precio_total_orden_compra *
               CASE O.K_Tipo_Moneda
                   WHEN 1 THEN 1
                   WHEN 2 THEN 20
                   WHEN 3 THEN 24
               END
           ) AS Monto_Total,
           COUNT(O.K_Orden_Compra) AS Cantidad_Compras
    FROM Ordenes_compra O
    INNER JOIN Proveedores P ON O.K_Proveedor = P.K_Proveedor
    WHERE O.F_Generacion >= '{fecha_inicio}' AND O.F_Generacion < '{fecha_fin}'
      AND O.B_Cerrada = 1 AND (O.B_Cancelada IS NULL OR O.B_Cancelada = 0)
    GROUP BY P.D_Proveedor
    ORDER BY Monto_Total DESC;
    """
    top_proveedores = ejecutar_consulta_sql(query_top_prov, fetchall=True)
    top_proveedores_json = [{"Nombre_Proveedor":p["D_Proveedor"],"Monto_Total":float(p["Monto_Total"]),"Cantidad_Compras":int(p["Cantidad_Compras"])} for p in top_proveedores]

    return {
        "resumen": resumen,
        "totales_monedas": {
            "pesos": float(totales["total_pesos"]),
            "dolares": float(totales["total_dolares"]),
            "euros": float(totales["total_euros"]),
            "total_general": float(totales["total_general_en_pesos"])
        },
        "top_proveedores": top_proveedores_json
    }

# ------------------- FRECUENCIA -------------------
@app.get("/valance/frecuencia")
def valance_frecuencia(mes: str = Query("", description="Mes en formato YYYY-MM"), access_token: str = Cookie(None)):
    if not access_token:
        return JSONResponse(status_code=401, content={"error": "No autorizado"})
    token = access_token.replace("Bearer ", "")
    payload = verificar_access_token(token)
    if not payload or not validar_acceso_valance(payload):
        return JSONResponse(status_code=403, content={"error": "Acceso denegado"})

    if not mes:
        mes = datetime.now().strftime("%Y-%m")
    fecha_inicio = datetime.strptime(f"{mes}-01","%Y-%m-%d")
    fecha_fin = (fecha_inicio.replace(month=fecha_inicio.month+1, day=1) 
                 if fecha_inicio.month<12 else fecha_inicio.replace(year=fecha_inicio.year+1, month=1, day=1))

    query_frecuencia = f"""
    SELECT TOP 10 P.D_Proveedor, COUNT(O.K_Orden_Compra) AS Cantidad_Compras, SUM(O.precio_total_orden_compra) AS Monto_Total
    FROM Ordenes_compra O
    INNER JOIN Proveedores P ON O.K_Proveedor = P.K_Proveedor
    WHERE O.F_Generacion >= '{fecha_inicio}' AND O.F_Generacion < '{fecha_fin}'
      AND O.B_Cerrada = 1 AND (O.B_Cancelada IS NULL OR O.B_Cancelada = 0)
    GROUP BY P.D_Proveedor
    ORDER BY Cantidad_Compras DESC;
    """
    top_proveedores = ejecutar_consulta_sql(query_frecuencia, fetchall=True)
    return {"top_proveedores":[{"Nombre_Proveedor":p["D_Proveedor"],"Cantidad_Compras":int(p["Cantidad_Compras"]),"Monto_Total":float(p["Monto_Total"])} for p in top_proveedores]}

# ------------------- DETALLE PROVEEDOR -------------------
@app.get("/valance/detalle_proveedor")
def detalle_proveedor(proveedor: str = Query(...), mes: str = Query("", description="Mes en formato YYYY-MM"), access_token: str = Cookie(None)):
    if not access_token:
        return JSONResponse(status_code=401, content={"error": "No autorizado"})
    token = access_token.replace("Bearer ", "")
    payload = verificar_access_token(token)
    if not payload or not validar_acceso_valance(payload):
        return JSONResponse(status_code=403, content={"error": "Acceso denegado"})

    if not mes:
        mes = datetime.now().strftime("%Y-%m")
    fecha_inicio = datetime.strptime(f"{mes}-01","%Y-%m-%d")
    fecha_fin = (fecha_inicio.replace(month=fecha_inicio.month+1, day=1) 
                 if fecha_inicio.month<12 else fecha_inicio.replace(year=fecha_inicio.year+1, month=1, day=1))

    query = f"""
    SELECT O.K_Orden_Compra, O.F_Generacion, O.precio_total_orden_compra AS Monto,
           CASE 
               WHEN O.B_Cerrada = 1 THEN 'Cerrada'
               ELSE 'Sin Autorizar'
           END AS Estado
    FROM Ordenes_compra O
    INNER JOIN Proveedores P ON O.K_Proveedor = P.K_Proveedor
    WHERE P.D_Proveedor = '{proveedor}'
      AND O.F_Generacion >= '{fecha_inicio}' AND O.F_Generacion < '{fecha_fin}'
      AND O.B_Cerrada = 1 AND (O.B_Cancelada IS NULL OR O.B_Cancelada = 0)
    ORDER BY O.F_Generacion ASC;
    """
    detalle = ejecutar_consulta_sql(query, fetchall=True)
    return [
        {
            "K_Orden_Compra": d["K_Orden_Compra"],
            "F_Generacion": d["F_Generacion"].strftime("%Y-%m-%d") if isinstance(d["F_Generacion"], datetime) else d["F_Generacion"],
            "Monto": float(d["Monto"]),
            "Estado": d["Estado"]
        } for d in detalle
    ]


# ------------------- LOGOUT -------------------
@app.get("/logout")
def logout():
    response = RedirectResponse(url="/", status_code=303)
    response.delete_cookie("access_token")
    return response