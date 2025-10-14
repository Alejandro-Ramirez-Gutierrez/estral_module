# routers/fabricacion_mensual.py
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

# Permisos
AREAS_PERMITIDAS = [20, 22]
EMPLEADOS_PERMITIDOS = [8811, 8661, 8870, 8740, 4, 5]

def validar_token(access_token: str):
    if not access_token:
        return None
    token = access_token.replace("Bearer ", "")
    payload = verificar_access_token(token)
    if not payload:
        return None
    
    if (payload.get("K_Area") in AREAS_PERMITIDAS) or (payload.get("K_Empleado") in EMPLEADOS_PERMITIDOS):
        return payload
    return None

# ---------------- PAGE ----------------
@router.get("/", response_class=HTMLResponse)
def direccion_page(request: Request, access_token: str = Cookie(None)):
    payload = validar_token(access_token)
    if not payload:
        return JSONResponse(status_code=403, content={"error":"Acceso denegado"})
    
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

    if not hasta:
        hasta = (datetime.today() + timedelta(days=1)).strftime("%Y-%m-%d")
    if not desde:
        dt = datetime.strptime(hasta, "%Y-%m-%d")
        desde = (dt.replace(day=1)).strftime("%Y-%m-%d")

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
    
    total_kg = sum([float(r["PesoTotal_Kilogramos"]) for r in rows])
    total_piezas = sum([int(r["Piezas"]) for r in rows])
    
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

    
    try:
        start_hour = int(bloque.split(":")[0])
    except:
        start_hour = None

   
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

# ---------------- API: Detalle Crudo por Área y Rango (para click en gráfica) ----------------
@router.get("/api/detalle_area")
def api_detalle_area(area: str = Query(...), desde: str = Query(...), hasta: str = Query(...), access_token: str = Cookie(None)):
    payload = validar_token(access_token)
    if not payload:
        return JSONResponse(status_code=403, content={"error":"Acceso denegado"})

    # Usamos DATEADD(DAY, 1, '{hasta}') para incluir todo el día 'hasta'
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
      AND mc.F_Movimiento >= '{desde}' AND mc.F_Movimiento < DATEADD(DAY, 1, '{hasta}')
    ORDER BY mc.F_Movimiento ASC
    """

    rows = ejecutar_consulta_sql(query, fetchall=True) or []
    result = []
    for d in rows:
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
        
        pdf = FPDF(orientation='P', unit='mm', format='A4')
        pdf.set_auto_page_break(True, margin=12)
        pdf.add_page()
        pdf.set_font("Arial", 'B', 14)
        pdf.cell(0, 10, nombre, ln=True, align="C")
        pdf.ln(4)
        pdf.set_font("Arial", size=9)
        
        cols = list(df.columns)
        page_w = pdf.w - 2 * pdf.l_margin
      
        if "Descripcion" in cols:
           
            desc_w = page_w * 0.4
            other_w = (page_w - desc_w) / (len(cols)-1) if len(cols) > 1 else page_w - desc_w
            widths = [other_w if c != "Descripcion" else desc_w for c in cols]
        else:
            widths = [page_w / len(cols) for _ in cols]
        
        for i, c in enumerate(cols):
            pdf.cell(widths[i], 8, str(c)[:30], 1)
        pdf.ln()
        
        for _, row in df.iterrows():
            max_h = 0
            y_start = pdf.get_y()
            x_start = pdf.get_x()

            for i, c in enumerate(cols):
                v = "" if pd.isna(row[c]) else str(row[c])
                if c == "Descripcion":
                   
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
        if not (area and dia and bloque):
            return JSONResponse(status_code=400, content={"error":"Faltan parámetros para detalle (area/dia/bloque)"})
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



@router.get("/api/perfilado/mensual")
def api_perfilado_mensual(desde: str = Query(...), hasta: str = Query(...), access_token: str = Cookie(None)):
    payload = validar_token(access_token)
    if not payload:
        return JSONResponse(status_code=403, content={"error": "Acceso denegado"})

    query = f"""
    SELECT
        LTRIM(RTRIM(
            LEFT(C.Descripcion,
                LEN(C.Descripcion) - CHARINDEX('-', REVERSE(C.Descripcion))
            )
        )) AS Descripcion_Corta,
        es.D_Estacion AS Estacion,
        SUM(mc.Cantidad) AS Cantidad_Fabricada,
        SUM(
            mc.Cantidad *
            CASE
                WHEN C.Descripcion LIKE '%LAMINA%' AND C.Descripcion LIKE '%x%' THEN
                    TRY_CAST(
                        TRIM(REPLACE(REPLACE(
                            SUBSTRING(
                                C.Descripcion,
                                CHARINDEX('x', C.Descripcion) + 1,
                                CHARINDEX('mm', C.Descripcion) - CHARINDEX('x', C.Descripcion) - 1
                            ), 'mm',''), '-', '')
                        ) AS FLOAT) / 1000
                ELSE
                    TRY_CAST(
                        TRIM(REPLACE(REPLACE(
                            SUBSTRING(
                                C.Descripcion,
                                LEN(C.Descripcion) - CHARINDEX('-', REVERSE(C.Descripcion)) + 2,
                                LEN(C.Descripcion)
                            ), 'mm',''), '-', '')
                        ) AS FLOAT) / 1000
            END
        ) AS Metros_Lineales_Fabricados,
        SUM(mc.Cantidad * C.Peso) / 1000 AS Peso_Fabricado_Tons
    FROM Movimientos_Componentes mc
    INNER JOIN Componentes_Partida C ON mc.K_Componente = C.K_Componente
    INNER JOIN Estacion es ON mc.K_Estacion = es.K_Estacion
    INNER JOIN Linea l ON es.K_Linea = l.K_Linea
    INNER JOIN Areas a ON l.K_Area = a.K_Area
    WHERE
        a.D_Area = 'PERFILADO'
        AND mc.K_Tipo_Movimiento = 2
        AND mc.F_Movimiento >= '{desde}' AND mc.F_Movimiento < '{hasta}'
    GROUP BY
        LTRIM(RTRIM(
            LEFT(C.Descripcion,
                LEN(C.Descripcion) - CHARINDEX('-', REVERSE(C.Descripcion))
            )
        )),
        es.D_Estacion
    ORDER BY es.D_Estacion, Metros_Lineales_Fabricados DESC;
    """

    rows = ejecutar_consulta_sql(query, fetchall=True) or []

    resumen = []
    total_metros = total_toneladas = total_piezas = 0

    for r in rows:
        metros = round(float(r["Metros_Lineales_Fabricados"] or 0), 2)
        peso = round(float(r["Peso_Fabricado_Tons"] or 0), 2)
        piezas = int(r["Cantidad_Fabricada"] or 0)

        resumen.append({
            "Descripcion_Corta": r["Descripcion_Corta"],
            "Estacion": r["Estacion"],
            "Cantidad_Fabricada": piezas,
            "Metros_Lineales_Fabricados": metros,
            "Peso_Fabricado_Tons": peso
        })

        total_metros += metros
        total_toneladas += peso
        total_piezas += piezas

    return {
        "kpis": {
            "total_metros": round(total_metros, 2),
            "total_toneladas": round(total_toneladas, 2),
            "total_piezas": total_piezas
        },
        "resumen_mensual_perfilado": resumen
    }


@router.get("/api/perfilado/diario")
def api_perfilado_diario(desde: str = Query(...), hasta: str = Query(...), access_token: str = Cookie(None)):
    payload = validar_token(access_token)
    if not payload:
        return JSONResponse(status_code=403, content={"error": "Acceso denegado"})

    query = f"""
    SELECT
        CONVERT(date, mc.F_Movimiento) AS Fecha_Fabricacion,
        LTRIM(RTRIM(
            LEFT(C.Descripcion,
                LEN(C.Descripcion) - CHARINDEX('-', REVERSE(C.Descripcion))
            )
        )) AS Descripcion_Corta,
        es.D_Estacion AS Estacion,
        SUM(mc.Cantidad) AS Cantidad_Fabricada,
        SUM(
            mc.Cantidad *
            CASE
                WHEN C.Descripcion LIKE '%LAMINA%' AND C.Descripcion LIKE '%x%' THEN
                    TRY_CAST(
                        TRIM(REPLACE(REPLACE(
                            SUBSTRING(
                                C.Descripcion,
                                CHARINDEX('x', C.Descripcion) + 1,
                                CHARINDEX('mm', C.Descripcion) - CHARINDEX('x', C.Descripcion) - 1
                            ), 'mm',''), '-', '')
                        ) AS FLOAT) / 1000
                ELSE
                    TRY_CAST(
                        TRIM(REPLACE(REPLACE(
                            SUBSTRING(
                                C.Descripcion,
                                LEN(C.Descripcion) - CHARINDEX('-', REVERSE(C.Descripcion)) + 2,
                                LEN(C.Descripcion)
                            ), 'mm',''), '-', '')
                        ) AS FLOAT) / 1000
            END
        ) AS Metros_Lineales_Fabricados,
        SUM(mc.Cantidad * C.Peso) / 1000 AS Peso_Fabricado_Tons
    FROM Movimientos_Componentes mc
    INNER JOIN Componentes_Partida C ON mc.K_Componente = C.K_Componente
    INNER JOIN Estacion es ON mc.K_Estacion = es.K_Estacion
    INNER JOIN Linea l ON es.K_Linea = l.K_Linea
    INNER JOIN Areas a ON l.K_Area = a.K_Area
    WHERE
        a.D_Area = 'PERFILADO'
        AND mc.K_Tipo_Movimiento = 2
        AND mc.F_Movimiento >= '{desde}' AND mc.F_Movimiento < '{hasta}'
    GROUP BY
        CONVERT(date, mc.F_Movimiento),
        LTRIM(RTRIM(
            LEFT(C.Descripcion,
                LEN(C.Descripcion) - CHARINDEX('-', REVERSE(C.Descripcion))
            )
        )),
        es.D_Estacion
    ORDER BY Fecha_Fabricacion ASC, es.D_Estacion, Metros_Lineales_Fabricados DESC;
    """

    rows = ejecutar_consulta_sql(query, fetchall=True) or []

    detalle = []
    total_metros = total_toneladas = total_piezas = 0

    for r in rows:
        metros = round(float(r["Metros_Lineales_Fabricados"] or 0), 2)
        peso = round(float(r["Peso_Fabricado_Tons"] or 0), 2)
        piezas = int(r["Cantidad_Fabricada"] or 0)

        detalle.append({
            "Fecha_Fabricacion": r["Fecha_Fabricacion"].strftime("%Y-%m-%d") if hasattr(r["Fecha_Fabricacion"], "strftime") else r["Fecha_Fabricacion"],
            "Descripcion_Corta": r["Descripcion_Corta"],
            "Estacion": r["Estacion"],
            "Cantidad_Fabricada": piezas,
            "Metros_Lineales_Fabricados": metros,
            "Peso_Fabricado_Tons": peso
        })

        total_metros += metros
        total_toneladas += peso
        total_piezas += piezas

    return {
        "kpis": {
            "total_metros": round(total_metros, 2),
            "total_toneladas": round(total_toneladas, 2),
            "total_piezas": total_piezas
        },
        "detalle_diario_perfilado": detalle
    }


@router.get("/api/export_json")
def api_export_json(
    mode: str = Query(...), 
    tipo: str = Query(...),
    desde: str = Query(...),
    hasta: str = Query(...),
    data: str = Query(...),
    columns: str = Query(...),
    access_token: str = Cookie(None)
):
    payload = validar_token(access_token)
    if not payload:
        return JSONResponse(status_code=403, content={"error": "Acceso denegado"})

    import json
    
    try:
        # 1. Deserializar los datos de la URL
        data_list = json.loads(data)
        columns_list = json.loads(columns)
    except json.JSONDecodeError:
        return JSONResponse(status_code=400, content={"error": "Error al decodificar datos JSON."})
    
    if not data_list:
        return JSONResponse(status_code=404, content={"error": "No hay datos para exportar."})
        
    # 2. Crear el DataFrame
    df = pd.DataFrame(data_list)
    
    # 3. Reordenar las columnas y formatear
    df = df[columns_list]
    
    if mode == "perfilado_mensual":
        nombre = f"Perfilado_Resumen_{desde}_a_{hasta}"
        df.columns = ["Descripción", "Estación", "Piezas", "Metros (m)", "Peso (Ton)"]
        
    elif mode == "perfilado_diario":
        nombre = f"Perfilado_DetalleDiario_{desde}_a_{hasta}"
        df.columns = ["Fecha", "Descripción", "Estación", "Piezas", "Metros (m)", "Peso (Ton)"]

    # 4. Generar la exportación usando la función existente
    return generar_export(df, nombre, tipo)
