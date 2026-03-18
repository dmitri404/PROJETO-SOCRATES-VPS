import os
from fastapi import Header, HTTPException

API_KEYS = {
    "portal_municipal_manaus": os.getenv("API_KEY_PORTAL_MUNICIPAL_MANAUS"),
    "portal_estado_am":        os.getenv("API_KEY_PORTAL_ESTADO_AM"),
}

def verificar_api_key(app: str, x_api_key: str = Header(...)):
    esperada = API_KEYS.get(app)
    if not esperada or x_api_key != esperada:
        raise HTTPException(status_code=401, detail="API key invalida")
