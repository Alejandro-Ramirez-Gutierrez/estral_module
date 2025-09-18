# services/db_service.py
from config import get_connection
import requests
import pyodbc
import os

def login_user(username: str, password: str, aplicacion: str = "EstralWeb", version: str = "1.1.5.18", b_web: int = 1):
    """
    Valida el login del usuario usando el procedure Gp_Valida_Usuario_Nuevo.
    Devuelve un diccionario con 'user' si es correcto o 'error' si falla.
    """
    conn = get_connection()
    cursor = conn.cursor()

    try:
        cursor.execute("""
            DECLARE @pmsMsg varchar(254);
            EXEC Gp_Valida_Usuario_Nuevo
                @Login=?,
                @Contrasenia=?,
                @Aplicacion=?,
                @Version=?,
                @B_WEB=?,
                @pmsMsg=@pmsMsg OUTPUT;
            SELECT @pmsMsg AS pmsMsg;
        """, username, password, aplicacion, version, b_web)

        # Leer primer result set (datos del usuario)
        user_row = cursor.fetchone()

        # Leer siguiente result set (mensaje del procedure)
        if cursor.nextset():
            output_row = cursor.fetchone()
            if output_row and output_row.pmsMsg:
                # Ignoramos el mensaje del procedure y devolvemos genérico
                return {"error": "Usuario o contraseña incorrectos"}

        if not user_row:
            return {"error": "Usuario o contraseña incorrectos"}

        # Construir diccionario con datos del usuario
        user_data = {
            "K_Usuario": user_row.K_Usuario,
            "D_Usuario": user_row.D_Usuario,
            "K_Empleado": user_row.K_Empleado,
            "D_Empleado": user_row.D_Empleado,
            "K_Oficina": user_row.K_Oficina,
            "D_Oficina": user_row.D_Oficina,
            "K_Empresa": user_row.K_Empresa,
            "D_Empresa": user_row.D_Empresa,
            "K_Area": getattr(user_row, "K_Area", None),
            "D_Area": getattr(user_row, "D_Area", None),
            "K_Departamento": getattr(user_row, "K_Departamento", None),
            "D_Departamento": getattr(user_row, "D_Departamento", None)
        }

        return {"user": user_data}

    except Exception:
        # No exponemos excepciones internas -> siempre mismo error
        return {"error": "Usuario o contraseña incorrectos"}

    finally:
        cursor.close()
        conn.close()


def valida_usuario(username: str, password: str):
    """
    Función para FastAPI que devuelve True si el login es correcto, False si falla.
    """
    result = login_user(username, password)
    return "user" in result

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

            # 👉 payload que espera el servicio
            payload = {
                "K_Orden_Compra": orden,
                "Tipo": tipo,
                "Code": code
            }

            #  llamada al servicio
            response = requests.post(
                "https://dev.altisconsultores.com.mx/wsEstral/getOrdenCompra",
                json=payload
            )

            pdf_path = ""
            if response.status_code == 200:
                # Guardar PDF temporalmente
                pdf_dir = "static/pdfs"
                os.makedirs(pdf_dir, exist_ok=True)
                pdf_path = os.path.join(pdf_dir, f"orden_{orden}.pdf")
                with open(pdf_path, "wb") as f:
                    f.write(response.content)

            result.append({
                "Orden": orden,
                "Estatus_OC": getattr(r, "D_Estado_Orden_Compra", "") or "",
                "Oficina": getattr(r, "D_Oficina_Genera", "") or "",
                "Cve_Prov": getattr(r, "K_Proveedor", 0) or 0,
                "Proveedor": getattr(r, "D_Proveedor", "") or "",
                "Total": getattr(r, "Precio_Total_Orden_Compra", 0) or 0,
                "Moneda": getattr(r, "C_Tipo_Moneda", "") or "",
                "PDF": pdf_path.replace("static/", "/static/"),  # 👉 servible por FastAPI
                "Empleado_Autoriza": getattr(r, "Empleado_Autoriza", "") or "",
                "Fecha_Generacion": getattr(r, "F_Generacion", "") or "",
                "Genero_Orden": getattr(r, "D_Empleado_Genera", "") or "",
                "Code": code
            })
        return result
    finally:
        cursor.close()
        conn.close()

# services/db_service.py

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
                "K_Motivo_Cancelacion_Orden": r.K_Motivo_Cancelacion_Orden,
                "D_Motivo_Cancelacion_Orden": r.D_Motivo_Cancelacion_Orden,
                "B_Activo": r.B_Activo
            }
            for r in rows
        ]
    finally:
        cursor.close()
        conn.close()



def cancelar_orden_compra(k_orden_compra: int, k_empleado: int, k_motivo: int = None):
    try:
        conn = get_connection()
        cursor = conn.cursor()

        cursor.execute("""
            DECLARE @rc INT;
            DECLARE @pmsMsg VARCHAR(200);
            
            EXEC @rc = Gp_Libera_Orden_Compras
                @K_Orden_Compra=?,
                @K_Empleado_Cancela=?,
                @Pmsmsg=@pmsMsg OUTPUT;
            
            SELECT @pmsMsg AS pmsMsg, @rc AS return_code;
        """, k_orden_compra, k_empleado)

        cursor.nextset()

        output_row = cursor.fetchone()
        
        pmsmsg = output_row.pmsMsg if output_row else None
        return_code = output_row.return_code if output_row else None

         
        if return_code == 0:
            return {"ok": pmsmsg}
        else:
          return {"error": pmsmsg}

    except Exception as e:
        return {"error": str(e)}
    finally:
        cursor.commit()
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()


def autorizar_orden(k_orden, k_empleado):
    """
    Llama al procedure GP_Autoriza_OrdenCompra para autorizar la orden.
    Devuelve: (B_Notificacion, Mensaje)
    Además hace print en consola para depuración.
    """
    conn = get_connection()
    cursor = conn.cursor()
    try:
        # Ejecutamos el procedure con OUTPUT
        cursor.execute("""
            DECLARE @B_Notificacion BIT, @Pmsmsg VARCHAR(2000);
            EXEC GP_Autoriza_OrdenCompra 
                @K_Orden_Compra=?, 
                @K_Empleado_Autoriza=?, 
                @B_Notificacion=@B_Notificacion OUTPUT, 
                @Pmsmsg=@Pmsmsg OUTPUT;
            SELECT @B_Notificacion AS B_Notificacion, @Pmsmsg AS Mensaje;
        """, k_orden, k_empleado)
        row = cursor.fetchone()
        b_notificacion = getattr(row, "B_Notificacion", 0)
        mensaje = getattr(row, "Mensaje", "")

        return b_notificacion, mensaje

    except Exception as e:
        return 0, str(e)

    finally:
        cursor.commit()
        cursor.close()
        conn.close()