# services/db_service.py
from config import get_connection,get_mysql_connection
import requests
import pyodbc
import os
import traceback

# -------------------- Helpers --------------------
def safe_fetch(cursor):
    """Obtiene un row de cursor devolviendo None si falla."""
    try:
        return cursor.fetchone()
    except Exception:
        return None

# -------------------- Login --------------------
def login_user(username: str, password: str, aplicacion: str = "EstralWeb",
               version: str = "1.1.5.18", b_web: int = 1):
    """
    Valida el login del usuario usando el procedure Gp_Valida_Usuario_Nuevo.
    Devuelve {'user': {...}} si es correcto o {'error': '...'} si falla.
    """
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            DECLARE @pmsMsg VARCHAR(254);
            EXEC Gp_Valida_Usuario_Nuevo
                @Login=?,
                @Contrasenia=?,
                @Aplicacion=?,
                @Version=?,
                @B_WEB=?,
                @pmsMsg=@pmsMsg OUTPUT;
            SELECT @pmsMsg AS pmsMsg;
        """, username, password, aplicacion, version, b_web)

        
        user_row = safe_fetch(cursor)

        pmsMsg = None
        if cursor.nextset():
            msg_row = safe_fetch(cursor)
            if msg_row and hasattr(msg_row, "pmsMsg"):
                pmsMsg = msg_row.pmsMsg

        if pmsMsg and len(pmsMsg.strip()) > 0:
            return {"error": pmsMsg}

        if not user_row or not getattr(user_row, "K_Usuario", None):
            return {"error": "Usuario o contraseña incorrectos"}

        user_data = {
            "K_Usuario": getattr(user_row, "K_Usuario", None),
            "D_Usuario": getattr(user_row, "D_Usuario", None),
            "K_Empleado": getattr(user_row, "K_Empleado", None),
            "D_Empleado": getattr(user_row, "D_Empleado", None),
            "K_Oficina": getattr(user_row, "K_Oficina", None),
            "D_Oficina": getattr(user_row, "D_Oficina", None),
            "K_Empresa": getattr(user_row, "K_Empresa", None),
            "D_Empresa": getattr(user_row, "D_Empresa", None),
            "K_Area": getattr(user_row, "K_Area", None),
            "D_Area": getattr(user_row, "D_Area", None),
            "K_Departamento": getattr(user_row, "K_Departamento", None),
            "D_Departamento": getattr(user_row, "D_Departamento", None)
        }

        return {"user": user_data}

    except Exception as e:
        print("ERROR LOGIN_USER:", e)
        return {"error": str(e)}

    finally:
        cursor.close()
        conn.close()


def valida_usuario(username: str, password: str):
    result = login_user(username, password)
    return "user" in result

# -------------------- Órdenes --------------------
def obtener_ordenes_para_autorizar(k_empleado: int):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("EXEC GP_Busca_OrdenCompraAutorizar @K_Empleado_Autoriza=?", k_empleado)
        rows = cursor.fetchall()
        result = []

        for r in rows:
            orden = getattr(r, "K_Orden_Compra", 0) or 0
            code = getattr(r, "Code", "") or ""
            tipo = "001"

            payload = {"K_Orden_Compra": orden, "Tipo": tipo, "Code": code}
            pdf_path = ""

            try:
                response = requests.post(
                    "https://dev.altisconsultores.com.mx/wsEstral/getOrdenCompra",
                    json=payload
                )
                if response.status_code == 200:
                    pdf_dir = "static/pdfs"
                    os.makedirs(pdf_dir, exist_ok=True)
                    pdf_path = os.path.join(pdf_dir, f"orden_{orden}.pdf")
                    with open(pdf_path, "wb") as f:
                        f.write(response.content)
            except Exception as e:
                print(f"ERROR descargando PDF orden {orden}: {e}")

            result.append({
                "Orden": orden,
                "Estatus_OC": getattr(r, "D_Estado_Orden_Compra", "") or "",
                "Oficina": getattr(r, "D_Oficina_Genera", "") or "",
                "Cve_Prov": getattr(r, "K_Proveedor", 0) or 0,
                "Proveedor": getattr(r, "D_Proveedor", "") or "",
                "Total": getattr(r, "Precio_Total_Orden_Compra", 0) or 0,
                "Moneda": getattr(r, "C_Tipo_Moneda", "") or "",
                "PDF": pdf_path.replace("static/", "/static/"),
                "Empleado_Autoriza": getattr(r, "Empleado_Autoriza", "") or "",
                "Fecha_Generacion": getattr(r, "F_Generacion", "") or "",
                "Genero_Orden": getattr(r, "D_Empleado_Genera", "") or "",
                "Code": code
            })
        return result
    finally:
        cursor.close()
        conn.close()

# -------------------- Motivos --------------------
def obtener_motivos_cancelacion(k_motivo: int = None, activo: int = 1):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            EXEC SK_Motivos_Cancelacion_Orden
                @K_MotivoCancelacionOrden=?,
                @B_Activo=?
        """, k_motivo, activo)
        rows = cursor.fetchall()
        return [
            {
                "K_Motivo_Cancelacion_Orden": getattr(r, "K_Motivo_Cancelacion_Orden", None),
                "D_Motivo_Cancelacion_Orden": getattr(r, "D_Motivo_Cancelacion_Orden", None),
                "B_Activo": getattr(r, "B_Activo", None)
            }
            for r in rows
        ]
    finally:
        cursor.close()
        conn.close()

# -------------------- Cancelar / Autorizar --------------------
def cancelar_orden_compra(k_orden_compra: int, k_empleado: int, k_motivo: int = None):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            DECLARE @rc INT;
            DECLARE @pmsMsg VARCHAR(200);
            EXEC @rc = Gp_Libera_Orden_Compras
                @K_Orden_Compra=?,
                @K_Empleado_Cancela=?,
                @Pmsmsg=@pmsMsg OUTPUT;
            SELECT @pmsMsg AS pmsMsg, @rc AS return_code;
        """, k_orden_compra, k_empleado)

        if cursor.nextset():
            output_row = safe_fetch(cursor)
            pmsmsg = getattr(output_row, "pmsMsg", None)
            return_code = getattr(output_row, "return_code", None)
        else:
            pmsmsg = None
            return_code = None

        conn.commit()
        if return_code == 0:
            return {"ok": pmsmsg}
        else:
            return {"error": pmsmsg}

    except Exception as e:
        print("ERROR cancelar_orden_compra:", e)
        return {"error": str(e)}
    finally:
        cursor.close()
        conn.close()

def autorizar_orden(k_orden, k_empleado):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            DECLARE @B_Notificacion BIT, @Pmsmsg VARCHAR(2000);
            EXEC GP_Autoriza_OrdenCompra 
                @K_Orden_Compra=?, 
                @K_Empleado_Autoriza=?, 
                @B_Notificacion=@B_Notificacion OUTPUT, 
                @Pmsmsg=@Pmsmsg OUTPUT;
            SELECT @B_Notificacion AS B_Notificacion, @Pmsmsg AS Mensaje;
        """, k_orden, k_empleado)
        row = safe_fetch(cursor)
        b_notificacion = getattr(row, "B_Notificacion", 0)
        mensaje = getattr(row, "Mensaje", "")
        conn.commit()
        return b_notificacion, mensaje

    except Exception as e:
        print("ERROR autorizar_orden:", e)
        return 0, str(e)
    finally:
        cursor.close()
        conn.close()

# -------------------- Consulta SQL genérica (¡AJUSTADA!) --------------------
def ejecutar_consulta_sql(query: str, params=None, fetchone: bool = False, fetchall: bool = False):
    """
    Ejecuta una consulta SQL genérica.
    Acepta 'params' (lista o tupla) para consultas parametrizadas seguras.
    """
    conn = get_connection()
    cursor = conn.cursor()
    try:
        if params:
            # ✅ Solución al Error 07002: Llama a execute con el query Y los parámetros.
            cursor.execute(query, params)
        else:
            # Para queries que no necesitan parámetros (como en tu dashboard)
            cursor.execute(query) 
            
        columns = [col[0] for col in cursor.description] if cursor.description else []
        
        if fetchone:
            row = safe_fetch(cursor)
            return dict(zip(columns, row)) if row else {}
        
        if fetchall:
            rows = cursor.fetchall()
            return [dict(zip(columns, r)) for r in rows]
            
        conn.commit()
        return {}
        
    except Exception as e:
        if fetchall:
             return [] 
        return {}
        
    finally:
        # Asegura el cierre del cursor y la conexión
        if cursor:
             cursor.close()
        if conn:
             conn.close()


# -------------------- Consulta SQL Genérica para MySQL (¡CORREGIDA!) --------------------
def ejecutar_consulta_mysql(query: str, params: tuple = None, fetchall: bool = True):
    """
    Ejecuta un query genérico en MySQL y devuelve resultados como lista de diccionarios.
    Solo hace COMMIT si la consulta no es un SELECT.
    """
    conn = None
    cursor = None
    try:
        # Asegúrate de que esta función está definida y obtiene una conexión
        conn = get_mysql_connection() 
        cursor = conn.cursor(dictionary=True)

        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)

        # 1. Fetch de resultados
        # Se leen todos los resultados para liberar el cursor, como ya lo hacías.
        resultados = cursor.fetchall() if fetchall else cursor.fetchone()

        # 2. COMMIT CONDICIONAL 🚨
        # Solo hacemos commit si la consulta NO es un SELECT, para evitar el error.
        if not query.strip().upper().startswith("SELECT"):
            conn.commit() 
        
        return resultados

    except Exception as e:
        # Se mantiene la lógica de manejo de errores y traceback
        print(f"ERROR ejecutar_consulta_mysql: {e}")
        return [] if fetchall else {}

    finally:
        # El cierre de cursor y conexión en el finally está CORRECTO.
        if cursor:
            cursor.close()
        if conn:
            conn.close()