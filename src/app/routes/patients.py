from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.orm import Session

from db import get_db

router = APIRouter(tags=["ingestion"])

@router.post("/ingest")
def ingest() -> dict:
    return {"status": "ok"}