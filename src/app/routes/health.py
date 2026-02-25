from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.orm import Session

from services.database import get_db

router = APIRouter(tags=["health"])

@router.get("/health")
def health_check() -> dict:
    return {"status": "ok"}


@router.get("/db-check")
def db_check(db: Session = Depends(get_db)) -> dict:
    result = db.execute(text("SELECT 1")).scalar()
    return {"db": "connected" if result == 1 else "unknown"}
