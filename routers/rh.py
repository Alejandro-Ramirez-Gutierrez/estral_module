from fastapi import APIRouter, Request, Cookie
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

    # -------------------- CONSULTA DE EMPLEADOS --------------------
    query_empleados = """
    SELECT e.K_Empleado, e.D_Empleado, e.Telefono,
           d.D_Departamento, d.K_Area, a.D_Area,
           p.D_Puesto,
           o.D_Oficina
    FROM Empleado e
    LEFT JOIN Departamento d ON e.K_Departamento = d.K_Departamento
    LEFT JOIN Puesto p ON e.K_Puesto = p.K_Puesto
    LEFT JOIN Oficina o ON e.K_Oficina = o.K_Oficina
    LEFT JOIN (
        SELECT DISTINCT K_Area, 'Área ' + CAST(K_Area AS VARCHAR) AS D_Area
        FROM Departamento
    ) a ON d.K_Area = a.K_Area
    WHERE e.B_Activo = 1
    ORDER BY e.D_Empleado
    """
    empleados = ejecutar_consulta_sql(query_empleados, fetchall=True)

    # -------------------- CONSULTA DE ÁREAS Y DEPARTAMENTOS --------------------
    query_areas = """
    SELECT d.K_Area, 'Área ' + CAST(d.K_Area AS VARCHAR) AS D_Area, d.K_Departamento, d.D_Departamento
    FROM Departamento d
    ORDER BY d.K_Area, d.D_Departamento
    """
    departamentos = ejecutar_consulta_sql(query_areas, fetchall=True)

    # Organizar departamentos por área para el template
    areas_dict = {}
    for dep in departamentos:
        k_area = dep["K_Area"]
        if k_area not in areas_dict:
            areas_dict[k_area] = {"D_Area": dep["D_Area"], "departamentos": []}
        areas_dict[k_area]["departamentos"].append({"K_Departamento": dep["K_Departamento"], "D_Departamento": dep["D_Departamento"]})
    areas = list(areas_dict.values())

    # -------------------- CONSULTA DE OFICINAS --------------------
    query_oficinas = """
    SELECT K_Oficina, D_Oficina, Calle, Numero_Externo, Codigo_Postal, Telefono
    FROM Oficina
    WHERE B_Activa = 1
    ORDER BY D_Oficina
    """
    oficinas = ejecutar_consulta_sql(query_oficinas, fetchall=True)

    # -------------------- RENDER TEMPLATE --------------------
    return templates.TemplateResponse(
        "rh.html",
        {
            "request": request,
            "usuario": payload.get("sub", "Usuario"),
            "empleados": empleados,
            "areas": areas,
            "oficinas": oficinas
        }
    )
