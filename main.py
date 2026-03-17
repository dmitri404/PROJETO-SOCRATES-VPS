from fastapi import FastAPI
from routers import aristoteles

app = FastAPI(title="Portal API", version="1.0.0")

app.include_router(aristoteles.router)

@app.get("/health")
def health():
    return {"status": "ok"}
