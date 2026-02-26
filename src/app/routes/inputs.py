import hashlib
import json
from services.temporal import start_csv_conversion
from typing import List

from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.orm import Session

from services.database import get_db
from models import IngestItem

router = APIRouter(tags=["ingestion"])

@router.post("/ingest")
async def ingest(
    payload: List[IngestItem],
    db: Session = Depends(get_db),
) -> dict:
    # Serialize the validated payload in a deterministic way for hashing
    normalized_payload = [
        {
            "mrn": item.mrn,
            "first_name": item.first_name,
            "last_name": item.last_name,
            "birth_date": item.birth_date.isoformat(),
            "visit_account_number": item.visit_account_number,
            "visit_date": item.visit_date.isoformat(),
            "reason": item.reason,
        }
        for item in payload
    ]
    payload_str = json.dumps(normalized_payload, sort_keys=True, separators=(",", ":"))
    md5_hash = hashlib.md5(payload_str.encode("utf-8")).hexdigest()

    # Check if this payload already exists
    existing_id = db.execute(
        text("SELECT id FROM ingestions WHERE md5_hash = :md5_hash"),
        {"md5_hash": md5_hash},
    ).scalar()

    if existing_id is not None:
        await start_csv_conversion(existing_id)
        return {"id": existing_id, "status": "existing"}

    # Insert new record
    new_id = db.execute(
        text(
            """
            INSERT INTO ingestions (payload, md5_hash)
            VALUES ( :payload, :md5_hash)
            RETURNING id
            """
        ),
        {"payload": payload_str, "md5_hash": md5_hash},
    ).scalar()
    db.commit()

    await start_csv_conversion(new_id)

    return {"id": new_id, "status": "created"}