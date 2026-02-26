from fastapi import FastAPI
from routes import routes
from services.database import initialize

app = FastAPI(title="ODI Exam API", version="0.1.0")

@app.on_event("startup")
async def startup_event() -> None:
    await initialize()

for route in routes:
    app.include_router(route)