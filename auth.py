import os
from fastapi import Header, HTTPException


API_KEYS = {
    "aristoteles": os.getenv("API_KEY_ARISTOTELES"),
}

def verificar_api_key(app: str, x_api_key: str = Header(...)):
    esperada = API_KEYS.get(app)
    if not esperada or x_api_key != esperada:
        raise HTTPException(status_code=401, detail="API key invalida")
