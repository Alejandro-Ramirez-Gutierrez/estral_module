# routers/fabricacion_direccion.py
from fastapi import APIRouter, Request, Query, Cookie
from fastapi.responses import JSONResponse, HTMLResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from utils.auth import verificar_access_token
from services.db_service import ejecutar_consulta_sql
from io import BytesIO
import pandas as pd
from datetime import datetime, timedelta

router = APIRouter()
templates = Jinja2Templates(directory="templates")

# Permisos (ajusta si necesitas)
AREAS_PERMITIDAS = [20, 22]
EMPLEADOS_PERMITIDOS = [8811, 8661, 8870, 8740, 4, 5]

def validar_token(access_token: str):
    if not access_token:
        return None
    token = access_token.replace("Bearer ", "")
    payload = verificar_access_token(token)
    if not payload:
        return None
    # validar acceso a fabricacion
    if (payload.get("K_Area") in AREAS_PERMITIDAS) or (payload.get("K_Empleado") in EMPLEADOS_PERMITIDOS):
        return payload
    return None

# ---------------- PAGE ----------------
@router.get("/", response_class=HTMLResponse)
def direccion_page(request: Request, access_token: str = Cookie(None)):
    payload = validar_token(access_token)
    if not payload:
        return JSONResponse(status_code=403, content={"error":"Acceso denegado"})
    # Default: último mes (desde primer día hasta primer día del siguiente mes)
    today = datetime.today()
    start_default = (today.replace(day=1) - timedelta(days=0)).strftime("%Y-%m-01")
    end_default = (today + timedelta(days=1)).strftime("%Y-%m-%d")
    return templates.TemplateResponse("fabricacion_mensual.html", {
        "request": request,
        "usuario": payload.get("sub", "Usuario"),
        "desde": start_default,
        "hasta": end_default
    })

# ---------------- API: resumen (agrupado) ----------------
@router.get("/api/resumen")
def api_resumen(desde: str = Query(None), hasta: str = Query(None), access_token: str = Cookie(None)):
    payload = validar_token(access_token)
    if not payload:
        return JSONResponse(status_code=403, content={"error":"Acceso denegado"})

    # defaults: mes actual
    if not hasta:
        hasta = (datetime.today() + timedelta(days=1)).strftime("%Y-%m-%d")
    if not desde:
        dt = datetime.strptime(hasta, "%Y-%m-%d")
        desde = (dt.replace(day=1)).strftime("%Y-%m-%d")

    # Query: usa la versión que concatena Tipos_Fabricados (STUFF) + peso/piezas
    query = f"""
    ;WITH Base AS (
      SELECT
        CASE WHEN DATEPART(HOUR, mc.F_Movimiento) >= 7 THEN CONVERT(date, mc.F_Movimiento)
             ELSE DATEADD(DAY, -1, CONVERT(date, mc.F_Movimiento)) END AS DiaTurno,
        a.D_Area AS Area_Produccion,
        CASE WHEN DATEPART(HOUR, mc.F_Movimiento) BETWEEN 7 AND 18 THEN 'Día' ELSE 'Noche' END AS Turno,
        ((DATEPART(HOUR, DATEADD(HOUR, -7, mc.F_Movimiento)) / 2) * 2) AS BloqueHour,
        mc.K_Estacion, mc.K_Componente, mc.K_Pedido, mc.K_Partida,
        mc.Cantidad, cp.Peso, mc.F_Movimiento
      FROM Movimientos_Componentes mc
      JOIN Componentes_Partida cp ON mc.K_Componente = cp.K_Componente
      JOIN Estacion mp ON mc.K_Estacion = mp.K_Estacion
      JOIN Linea l ON mp.K_Linea = l.K_Linea
      JOIN Areas a ON l.K_Area = a.K_Area
      WHERE mc.K_Tipo_Movimiento = 2
        AND mc.F_Movimiento >= '{desde}' AND mc.F_Movimiento < '{hasta}'
    )
    SELECT
      DiaTurno,
      Area_Produccion,
      Turno,
      -- formato de bloque horario legible
      CONCAT(RIGHT('0' + CAST((BloqueHour + 7) % 24 AS VARCHAR(2)),2), ':00 - ',
             RIGHT('0' + CAST((BloqueHour + 9) % 24 AS VARCHAR(2)),2), ':00') AS Bloque_Horas,
      SUM(Peso * Cantidad) AS PesoTotal_Kilogramos,
      SUM(Cantidad) AS Piezas,
      STUFF((
        SELECT DISTINCT ', ' + ISNULL(va.Descripcion,'') 
        FROM Base b2
        JOIN Componentes_Partida cp2 ON b2.K_Componente = cp2.K_Componente
        LEFT JOIN VW_Articulos_Todos va ON cp2.SKU = va.SKU
        WHERE b2.Area_Produccion = b.Area_Produccion
          AND b2.DiaTurno = b.DiaTurno
          AND b2.Turno = b.Turno
          AND b2.BloqueHour = b.BloqueHour
        FOR XML PATH(''), TYPE
      ).value('.', 'NVARCHAR(MAX)'), 1, 2, '') AS Tipos_Fabricados
    FROM Base b
    GROUP BY DiaTurno, Area_Produccion, Turno, BloqueHour
    ORDER BY DiaTurno DESC, Turno ASC, BloqueHour, Area_Produccion;
    """

    rows = ejecutar_consulta_sql(query, fetchall=True) or []
    # prepare KPIs
    total_kg = sum([float(r["PesoTotal_Kilogramos"]) for r in rows])
    total_piezas = sum([int(r["Piezas"]) for r in rows])
    # build response lists
    resumen = []
    for r in rows:
        resumen.append({
            "DiaTurno": r["DiaTurno"].strftime("%Y-%m-%d") if hasattr(r["DiaTurno"], "strftime") else r["DiaTurno"],
            "Area": r["Area_Produccion"],
            "Turno": r["Turno"],
            "Bloque": r["Bloque_Horas"],
            "PesoKg": float(r["PesoTotal_Kilogramos"] or 0),
            "Piezas": int(r["Piezas"] or 0),
            "Tipos": r["Tipos_Fabricados"] or ""
        })
    return {"kpis": {"total_kg": total_kg, "total_piezas": total_piezas}, "resumen": resumen}

# ---------------- API: detalle por bloque/area/dia ----------------
@router.get("/api/detalle")
def api_detalle(area: str = Query(...), dia: str = Query(...), bloque: str = Query(...), access_token: str = Cookie(None)):
    payload = validar_token(access_token)
    if not payload:
        return JSONResponse(status_code=403, content={"error":"Acceso denegado"})

    # bloque: "07:00 - 09:00" -> we extract start hour
    try:
        start_hour = int(bloque.split(":")[0])
    except:
        start_hour = None

    # Use a safe-ish query: match by computed BloqueHour similar to resumen
    query = f"""
    SELECT
      p.Pedido_Estral AS Pedido,
      pa.Partida_Estral AS Partida,
      cop.No AS Componente,
      cp.Descripcion,
      mc.Cantidad,
      mc.F_Movimiento AS Fecha,
      mp.D_Estacion AS Maquina,
      a.D_Area AS Area_Produccion,
      cp.Peso,
      (cp.Peso * mc.Cantidad) AS PesoTotal
    FROM Movimientos_Componentes mc
    JOIN Componentes_Partida cp ON mc.K_Componente = cp.K_Componente
    JOIN Estacion mp ON mc.K_Estacion = mp.K_Estacion
    JOIN Linea l ON mp.K_Linea = l.K_Linea
    JOIN Areas a ON l.K_Area = a.K_Area
    LEFT JOIN Pedidos p ON mc.K_Pedido = p.K_Pedido
    JOIN Partidas pa ON mc.K_Partida = pa.K_Partida
    JOIN Componentes_Partida cop ON mc.K_Componente = cop.K_Componente
    WHERE mc.K_Tipo_Movimiento = 2
      AND a.D_Area = '{area}'
      AND (CASE WHEN DATEPART(HOUR, mc.F_Movimiento) >= 7 THEN CONVERT(date, mc.F_Movimiento)
                 ELSE DATEADD(DAY, -1, CONVERT(date, mc.F_Movimiento)) END) = '{dia}'
    """
    if start_hour is not None:
        # compute the same block formula (DATEADD(HOUR,-7,mc.F_Movimiento) hour /2 *2 ) = bloqueHour
        query += f" AND ((DATEPART(HOUR, DATEADD(HOUR, -7, mc.F_Movimiento)) / 2) * 2) = { (start_hour - 7) % 24 if start_hour is not None else '((DATEPART(HOUR, DATEADD(HOUR, -7, mc.F_Movimiento)) / 2) * 2)'}"
    query += " ORDER BY mc.F_Movimiento ASC"

    data = ejecutar_consulta_sql(query, fetchall=True) or []
    result = []
    for d in data:
        result.append({
            "Pedido": d["Pedido"],
            "Partida": d["Partida"],
            "Componente": d["Componente"],
            "Descripcion": d["Descripcion"],
            "Cantidad": d["Cantidad"],
            "Peso": float(d["Peso"] or 0),
            "PesoTotal": float(d["PesoTotal"] or 0),
            "Fecha": d["Fecha"].strftime("%Y-%m-%d %H:%M:%S") if hasattr(d["Fecha"], "strftime") else d["Fecha"],
            "Maquina": d["Maquina"],
            "Area": d["Area_Produccion"]
        })
    return {"detalle": result}

# ---------------- EXPORT (resumen / detalle) ----------------
def generar_export(df: pd.DataFrame, nombre: str, tipo: str):
    from fpdf import FPDF
    if tipo == "excel":
        out = BytesIO()
        df.to_excel(out, index=False)
        out.seek(0)
        return StreamingResponse(out, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                headers={"Content-Disposition": f"attachment; filename={nombre}.xlsx"})
    elif tipo == "pdf":
        # Vertical A4, con multi_cell para Descripcion y ajuste dinámico de anchos
        pdf = FPDF(orientation='P', unit='mm', format='A4')
        pdf.set_auto_page_break(True, margin=12)
        pdf.add_page()
        pdf.set_font("Arial", 'B', 14)
        pdf.cell(0, 10, nombre, ln=True, align="C")
        pdf.ln(4)
        pdf.set_font("Arial", size=9)
        # determine columns: if Descripcion in df, give it more width
        cols = list(df.columns)
        page_w = pdf.w - 2 * pdf.l_margin
        # allocate widths
        if "Descripcion" in cols:
            # give description ~40% width, the rest share remaining
            desc_w = page_w * 0.4
            other_w = (page_w - desc_w) / (len(cols)-1) if len(cols) > 1 else page_w - desc_w
            widths = [other_w if c != "Descripcion" else desc_w for c in cols]
        else:
            widths = [page_w / len(cols) for _ in cols]
        # header
        for i, c in enumerate(cols):
            pdf.cell(widths[i], 8, str(c)[:30], 1)
        pdf.ln()
        # rows
        for _, row in df.iterrows():
            max_h = 0
            y_start = pdf.get_y()
            x_start = pdf.get_x()
            # write each cell; for Description use multi_cell
            for i, c in enumerate(cols):
                v = "" if pd.isna(row[c]) else str(row[c])
                if c == "Descripcion":
                    # save current x,y and write multi_cell with border handling
                    x = pdf.get_x()
                    y = pdf.get_y()
                    pdf.multi_cell(widths[i], 5, v, border=1)
                    h_after = pdf.get_y() - y
                    if h_after > max_h: max_h = h_after
                    pdf.set_xy(x + widths[i], y)
                else:
                    pdf.cell(widths[i], 6, v[:30], 1)
                    if 6 > max_h: max_h = 6
            pdf.ln(max_h)
        output = BytesIO(pdf.output(dest='S').encode('latin1'))
        output.seek(0)
        return StreamingResponse(output, media_type="application/pdf",
                                headers={"Content-Disposition": f"attachment; filename={nombre}.pdf"})
    else:
        return JSONResponse(status_code=400, content={"error":"Tipo inválido"})

@router.get("/api/export")
def api_export(mode: str = Query("resumen"), tipo: str = Query("excel"),
               desde: str = Query(None), hasta: str = Query(None),
               area: str = Query(None), dia: str = Query(None), bloque: str = Query(None),
               access_token: str = Cookie(None)):
    payload = validar_token(access_token)
    if not payload:
        return JSONResponse(status_code=403, content={"error":"Acceso denegado"})

    if mode == "resumen":
        # reutiliza api_resumen query but fetch raw and export
        if not hasta:
            hasta = (datetime.today() + timedelta(days=1)).strftime("%Y-%m-%d")
        if not desde:
            dt = datetime.strptime(hasta, "%Y-%m-%d")
            desde = dt.replace(day=1).strftime("%Y-%m-%d")
        q = f"""
        ;WITH Base AS (
          SELECT
            CASE WHEN DATEPART(HOUR, mc.F_Movimiento) >= 7 THEN CONVERT(date, mc.F_Movimiento)
                 ELSE DATEADD(DAY, -1, CONVERT(date, mc.F_Movimiento)) END AS DiaTurno,
            a.D_Area AS Area_Produccion,
            CASE WHEN DATEPART(HOUR, mc.F_Movimiento) BETWEEN 7 AND 18 THEN 'Día' ELSE 'Noche' END AS Turno,
            ((DATEPART(HOUR, DATEADD(HOUR, -7, mc.F_Movimiento)) / 2) * 2) AS BloqueHour,
            mc.K_Componente, mc.Cantidad, cp.Peso, mc.F_Movimiento
          FROM Movimientos_Componentes mc
          JOIN Componentes_Partida cp ON mc.K_Componente = cp.K_Componente
          JOIN Estacion mp ON mc.K_Estacion = mp.K_Estacion
          JOIN Linea l ON mp.K_Linea = l.K_Linea
          JOIN Areas a ON l.K_Area = a.K_Area
          WHERE mc.K_Tipo_Movimiento = 2
            AND mc.F_Movimiento >= '{desde}' AND mc.F_Movimiento < '{hasta}'
        )
        SELECT
          DiaTurno, Area_Produccion AS Area, Turno,
          CONCAT(RIGHT('0' + CAST((BloqueHour + 7) % 24 AS VARCHAR(2)),2), ':00 - ',
                 RIGHT('0' + CAST((BloqueHour + 9) % 24 AS VARCHAR(2)),2), ':00') AS Bloque,
          SUM(Peso * Cantidad) AS PesoKg,
          SUM(Cantidad) AS Piezas,
          STUFF((
            SELECT DISTINCT ', ' + ISNULL(va2.Descripcion,'')
            FROM Base b2
            JOIN Componentes_Partida cp2 ON b2.K_Componente = cp2.K_Componente
            LEFT JOIN VW_Articulos_Todos va2 ON cp2.SKU = va2.SKU
            WHERE b2.Area_Produccion = b.Area_Produccion
              AND b2.DiaTurno = b.DiaTurno
              AND b2.Turno = b.Turno
              AND b2.BloqueHour = b.BloqueHour
            FOR XML PATH(''), TYPE
          ).value('.', 'NVARCHAR(MAX)'), 1, 2, '') AS Tipos
        FROM Base b
        GROUP BY DiaTurno, Area_Produccion, Turno, BloqueHour
        ORDER BY DiaTurno DESC, Turno, BloqueHour, Area_Produccion;
        """
        rows = ejecutar_consulta_sql(q, fetchall=True) or []
        df = pd.DataFrame([{
            "DiaTurno": r["DiaTurno"].strftime("%Y-%m-%d") if hasattr(r["DiaTurno"], "strftime") else r["DiaTurno"],
            "Area": r["Area"],
            "Turno": r["Turno"],
            "Bloque": r["Bloque"],
            "PesoKg": float(r["PesoKg"] or 0),
            "Piezas": int(r["Piezas"] or 0),
            "Tipos": r["Tipos"] or ""
        } for r in rows])
        return generar_export(df, f"resumen_fabricacion_{desde}_a_{hasta}", tipo)

    elif mode == "detalle":
        # require area/dia/bloque
        if not (area and dia and bloque):
            return JSONResponse(status_code=400, content={"error":"Faltan parámetros para detalle (area/dia/bloque)"})
        # call same logic as api_detalle
        q = f"""
        SELECT
          p.Pedido_Estral AS Pedido,
          pa.Partida_Estral AS Partida,
          cop.No AS Componente,
          cp.Descripcion,
          mc.Cantidad,
          mc.F_Movimiento AS Fecha,
          mp.D_Estacion AS Maquina,
          a.D_Area AS Area_Produccion,
          cp.Peso,
          (cp.Peso * mc.Cantidad) AS PesoTotal
        FROM Movimientos_Componentes mc
        JOIN Componentes_Partida cp ON mc.K_Componente = cp.K_Componente
        JOIN Estacion mp ON mc.K_Estacion = mp.K_Estacion
        JOIN Linea l ON mp.K_Linea = l.K_Linea
        JOIN Areas a ON l.K_Area = a.K_Area
        LEFT JOIN Pedidos p ON mc.K_Pedido = p.K_Pedido
        JOIN Partidas pa ON mc.K_Partida = pa.K_Partida
        JOIN Componentes_Partida cop ON mc.K_Componente = cop.K_Componente
        WHERE mc.K_Tipo_Movimiento = 2
          AND a.D_Area = '{area}'
          AND (CASE WHEN DATEPART(HOUR, mc.F_Movimiento) >= 7 THEN CONVERT(date, mc.F_Movimiento)
                 ELSE DATEADD(DAY, -1, CONVERT(date, mc.F_Movimiento)) END) = '{dia}'
        """
        # compute start hour like before
        try:
            start_hour = int(bloque.split(":")[0])
            q += f" AND ((DATEPART(HOUR, DATEADD(HOUR, -7, mc.F_Movimiento)) / 2) * 2) = { (start_hour - 7) % 24 }"
        except:
            pass
        q += " ORDER BY mc.F_Movimiento ASC"
        rows = ejecutar_consulta_sql(q, fetchall=True) or []
        df = pd.DataFrame([{
            "Pedido": r["Pedido"],
            "Partida": r["Partida"],
            "Componente": r["Componente"],
            "Descripcion": r["Descripcion"],
            "Cantidad": r["Cantidad"],
            "Peso": float(r["Peso"] or 0),
            "PesoTotal": float(r["PesoTotal"] or 0),
            "Fecha": r["Fecha"].strftime("%Y-%m-%d %H:%M:%S") if hasattr(r["Fecha"], "strftime") else r["Fecha"],
            "Maquina": r["Maquina"],
            "Area": r["Area_Produccion"]
        } for r in rows])
        return generar_export(df, f"detalle_{area}_{dia}_{bloque.replace(' ','')}", tipo)

    else:
        return JSONResponse(status_code=400, content={"error":"mode debe ser resumen o detalle"})
