# estral_modulo/main.py
# -------------------- Encender entorno virtual --------------------
# macOS: source venv/bin/activate
# Windows PS: .\venv\Scripts\Activate.ps1
# Levantar servidor: uvicorn main:app --reload

from fastapi import FastAPI, Request, Form, Cookie, Body, Query
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from routers import auth_requisiciones, planeacion, fabricacion_mensual, fabricacion_mensual_partidas, quejas, embarques, cotizaciones
from routers import asignacion_equipos
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


# -------------------- APP --------------------
app = FastAPI(title="Estral Módulo - Autorización Requisiciones")
templates = Jinja2Templates(directory="templates")
app.mount("/static", StaticFiles(directory="static"), name="static")

# -------------------- ROUTERS --------------------
app.include_router(auth_requisiciones.router, prefix="/auth", tags=["Autenticación"])
app.include_router(planeacion.router, prefix="/planeacion", tags=["Planeación"])
app.include_router(fabricacion_mensual.router, prefix="/fabricacion", tags=["Fabricación Mensual"])
app.include_router(fabricacion_mensual_partidas.router, prefix="/fabricacion_partidas", tags=["Fabricación Mensual Partidas"])
app.include_router(quejas.router, prefix="/quejas", tags=["Quejas"])
app.include_router(embarques.router)
app.include_router(cotizaciones.router, prefix="/cotizaciones", tags=["Cotizaciones"])
app.include_router(asignacion_equipos.router, prefix="/asignacion_equipos", tags=["Asignación de Equipos"])


# -------------------- LOGIN --------------------
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
    return templates.TemplateResponse(
        "login.html",
        {"request": request, "error": resultado.get("error", "Usuario o contraseña incorrectos")}
    )


# -------------------- DASHBOARD --------------------
@app.get("/dashboard", response_class=HTMLResponse)
def dashboard(request: Request, access_token: str = Cookie(None)):
    if not access_token:
        return RedirectResponse(url="/", status_code=303)
    token = access_token.replace("Bearer ", "")
    payload = verificar_access_token(token)
    if not payload:
        return RedirectResponse(url="/", status_code=303)

    k_empleado = payload.get("K_Empleado")
    ordenes = obtener_ordenes_para_autorizar(k_empleado) if k_empleado else []
    return templates.TemplateResponse(
        "dashboard.html",
        {"request": request, "usuario": payload.get("sub", "Usuario"), "K_Empleado": k_empleado, "ordenes": ordenes}
    )


# -------------------- API: MOTIVOS CANCELACIÓN --------------------
@app.get("/dashboard/motivos_cancelacion")
def api_motivos_cancelacion(access_token: str = Cookie(None)):
    if not access_token:
        return JSONResponse(status_code=401, content={"error": "No autorizado"})
    token = access_token.replace("Bearer ", "")
    payload = verificar_access_token(token)
    if not payload:
        return JSONResponse(status_code=401, content={"error": "No autorizado"})
    return {"motivos": obtener_motivos_cancelacion()}


# -------------------- API: CANCELAR ORDEN --------------------
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


# -------------------- API: AUTORIZAR ORDEN --------------------
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


# -------------------- VALANCE --------------------
def validar_acceso_valance(payload):
    k_empleado = payload.get("K_Empleado")
    k_area = payload.get("K_Area")
    AREAS_PERMITIDAS = [99, 20, 22, 23]
    EMPLEADOS_PERMITIDOS = [8811, 8661, 8870, 8740, 4, 5]
    EMPLEADOS_PERMITIDOS = [8811,8611, 8661, 8870, 8740,4,5]
    return (k_area in AREAS_PERMITIDAS) or (k_empleado in EMPLEADOS_PERMITIDOS)

def calcular_rango_mes(mes: str):
    """Devuelve fecha_inicio y fecha_fin para un mes dado (YYYY-MM)"""
    fecha_inicio = datetime.strptime(f"{mes}-01", "%Y-%m-%d")
    if fecha_inicio.month < 12:
        fecha_fin = fecha_inicio.replace(month=fecha_inicio.month+1, day=1)
    else:
        fecha_fin = fecha_inicio.replace(year=fecha_inicio.year+1, month=1, day=1)
    return fecha_inicio, fecha_fin


@app.get("/valance", response_class=HTMLResponse)
def valance(request: Request, access_token: str = Cookie(None)):
    if not access_token:
        return RedirectResponse(url="/", status_code=303)
    token = access_token.replace("Bearer ", "")
    payload = verificar_access_token(token)
    if not payload or not validar_acceso_valance(payload):
        return JSONResponse(status_code=403, content={"error": "Acceso denegado"})

    today = datetime.today()
    meses = [{"value": f"{today.year}-{str(m).zfill(2)}", "name": f"{month_name[m]} {today.year}"} for m in range(1, today.month+1)]
    return templates.TemplateResponse(
        "valance.html",
        {
            "request": request,
            "usuario": payload.get("sub", "Usuario"),
            "K_Empleado": payload.get("K_Empleado"),
            "K_Area": payload.get("K_Area"),
            "meses": meses,
            "mes_actual": f"{today.year}-{str(today.month).zfill(2)}",
            "data": {"Total_Ordenes":0,"Cerradas":0,"Sin_Autorizar":0,"Canceladas":0,"Recepcionadas_Completas":0,"Recepcionadas_Parciales":0,"Sin_Recepcion":0},
            "top_proveedores": []
        }
    )


# -------------------- DATOS VALANCE --------------------
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
    fecha_inicio, fecha_fin = calcular_rango_mes(mes)

    # Totales por moneda
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

    # Resumen
    anio, mes_num = map(int, mes.split("-"))
    resumen_sp = ejecutar_consulta_sql(f"EXEC SK_Reporte_Ordenes_Compra_Resumen @Anio={anio}, @Mes={mes_num}", fetchone=True)
    resumen = {
        "Total_Ordenes": resumen_sp["Todas"],
        "Cerradas": resumen_sp["Autorizadas"],
        "Sin_Autorizar": resumen_sp["SinAutorizar"],
        "Canceladas": resumen_sp["Canceladas"],
        "Recepcionadas_Completas": resumen_sp["Completas"],
        "Recepcionadas_Parciales": resumen_sp["Parciales"],
        "Sin_Recepcion": resumen_sp["SinRecepcion"]
    }

    # Top proveedores
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
    top_proveedores_json = [{"Nombre_Proveedor":p["D_Proveedor"], "Monto_Total":float(p["Monto_Total"]), "Cantidad_Compras":int(p["Cantidad_Compras"])} for p in top_proveedores]

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


# -------------------- FRECUENCIA --------------------
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
    fecha_inicio, fecha_fin = calcular_rango_mes(mes)

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


# -------------------- DETALLE PROVEEDOR --------------------
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
    fecha_inicio, fecha_fin = calcular_rango_mes(mes)

    query = f"""
    SELECT O.K_Orden_Compra, O.F_Generacion, O.precio_total_orden_compra AS Monto,
           CASE WHEN O.B_Cerrada = 1 THEN 'Cerrada' ELSE 'Sin Autorizar' END AS Estado
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

# -------------------- GRAFICA FAMILIA --------------------
@app.get("/valance/familia")
def valance_familia(mes: str = Query("", description="Mes en formato YYYY-MM"), access_token: str = Cookie(None)):
    if not access_token:
        return JSONResponse(status_code=401, content={"error": "No autorizado"})
    token = access_token.replace("Bearer ", "")
    payload = verificar_access_token(token)
    if not payload or not validar_acceso_valance(payload):
        return JSONResponse(status_code=403, content={"error": "Acceso denegado"})

    if not mes:
        mes = datetime.now().strftime("%Y-%m")
    fecha_inicio, fecha_fin = calcular_rango_mes(mes)

    query = f"""
    SELECT T.D_Familia_Articulo AS Familia, SUM(T.Total) AS Total
    FROM (
        SELECT O.K_Orden_Compra, SUM(D.Total) AS Total, T.D_Familia_Articulo
        FROM Ordenes_Compra O
        JOIN Detalle_Ordenes_Compra D ON O.K_Orden_Compra = D.K_Orden_Compra
        JOIN VW_Articulos_Todos T ON D.SKU = T.SKU
        WHERE ISNULL(O.B_Cancelada,0)=0 AND ISNULL(O.B_Completa,0)=0
          AND O.F_Generacion >= '{fecha_inicio}' AND O.F_Generacion < '{fecha_fin}'
        GROUP BY O.K_Orden_Compra, T.D_Familia_Articulo
    ) T
    GROUP BY T.D_Familia_Articulo
    ORDER BY T.D_Familia_Articulo;
    """
    data = ejecutar_consulta_sql(query, fetchall=True)
    return [{"Familia": d["Familia"].strip(), "Total": float(d["Total"])} for d in data]


# -------------------- LOGOUT --------------------
@app.get("/logout")
def logout():
    response = RedirectResponse(url="/", status_code=303)
    response.delete_cookie("access_token")
    return response
