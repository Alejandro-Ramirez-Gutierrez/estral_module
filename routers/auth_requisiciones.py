# estral_modulo/routers/auth_requisiciones.py
from fastapi import APIRouter, HTTPException
from services.db_service import valida_usuario

router = APIRouter()

@router.post("/login")
def login_usuario(login: str, contrasenia: str):
    es_valido = valida_usuario(login, contrasenia)
    if not es_valido:
        raise HTTPException(status_code=401, detail="Usuario o contraseña incorrectos")
    return {"message": "Login exitoso, bienvenido al dashboard"}