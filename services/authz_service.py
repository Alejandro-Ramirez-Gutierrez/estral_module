# services/authz_service.py

# Áreas permitidas
AREAS_PERMITIDAS = {99, 20, 22, 23}

# Usuarios específicos permitidos
USUARIOS_PERMITIDOS = {8811, 8870, 8740}

def puede_ver_dashboard(user: dict) -> bool:
    """
    Valida acceso al dashboard:
    - Si el K_Area está en AREAS_PERMITIDAS
    - O si el K_Empleado está en USUARIOS_PERMITIDOS
    """
    if not user:
        return False

    if user.get("K_Area") in AREAS_PERMITIDAS:
        return True

    if user.get("K_Empleado") in USUARIOS_PERMITIDOS:
        return True

    return False