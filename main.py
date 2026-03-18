from fastapi import FastAPI
from routers import portal_municipal_manaus, portal_estado_am

app = FastAPI(title="Portal API", version="1.0.0")

app.include_router(portal_municipal_manaus.router)
app.include_router(portal_estado_am.router)

@app.get("/health")
def health():
    return {"status": "ok"}
