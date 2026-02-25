import json
import os
from pathlib import Path

import boto3
from sqlalchemy import text
from temporalio import activity

from services.database import SessionLocal
from services.s3 import upload_csv

UPLOAD_DIR = Path(os.getenv("UPLOADS_DIR", "uploads"))

@activity.defn
async def get_ingestion(entry_id: int) -> dict | None:
    db = SessionLocal()
    try:
        row = (
            db.execute(
                text(
                    """
                    SELECT id, payload, status, csv_filename, s3_path
                    FROM ingestions
                    WHERE id = :id
                    """
                ),
                {"id": entry_id},
            )
            .mappings()
            .first()
        )
        return dict(row) if row is not None else None
    finally:
        db.close()


@activity.defn
async def convert_to_csv_and_mark_converted(entry_id: int) -> str:
    db = SessionLocal()
    try:
        row = (
            db.execute(
                text(
                    "SELECT id, payload, status FROM ingestions WHERE id = :id"
                ),
                {"id": entry_id},
            )
            .mappings()
            .first()
        )
        if row is None:
            raise RuntimeError(f"Ingestion {entry_id} not found")

        if row["status"] != "new":
            return row["status"]

        raw_payload = row["payload"]
        if isinstance(raw_payload, str):
            payload = json.loads(raw_payload)
        else:
            payload = raw_payload or []

        UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
        filename = f"ingestion_{entry_id}.csv"
        filepath = UPLOAD_DIR / filename

        headers = [
            "mrn",
            "first_name",
            "last_name",
            "birth_date",
            "visit_account_number",
            "visit_date",
            "reason",
        ]

        # Write CSV file
        import csv

        with filepath.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            for item in payload:
                writer.writerow(
                    {
                        "mrn": item.get("mrn"),
                        "first_name": item.get("first_name"),
                        "last_name": item.get("last_name"),
                        "birth_date": item.get("birth_date"),
                        "visit_account_number": item.get(
                            "visit_account_number"
                        ),
                        "visit_date": item.get("visit_date"),
                        "reason": item.get("reason"),
                    }
                )

        db.execute(
            text(
                """
                UPDATE ingestions
                SET status = 'converted',
                    csv_filename = :filename
                WHERE id = :id
                """
            ),
            {"id": entry_id, "filename": filename},
        )
        db.commit()

        return "converted"
    finally:
        db.close()


@activity.defn
async def upload_csv_to_s3_and_mark_uploaded(entry_id: int) -> str:
    db = SessionLocal()
    try:
        row = (
            db.execute(
                text(
                    """
                    SELECT id, status, csv_filename
                    FROM ingestions
                    WHERE id = :id
                    """
                ),
                {"id": entry_id},
            )
            .mappings()
            .first()
        )
        if row is None:
            raise RuntimeError(f"Ingestion {entry_id} not found")

        if row["status"] != "converted":
            return row["status"]

        filename = row["csv_filename"]
        if not filename:
            raise RuntimeError(
                f"Ingestion {entry_id} has no CSV filename recorded"
            )

        filepath = UPLOAD_DIR / filename
        if not filepath.exists():
            raise RuntimeError(
                f"CSV file for ingestion {entry_id} not found at {filepath}"
            )

        s3_path = upload_csv(filepath, filename)

        db.execute(
            text(
                """
                UPDATE ingestions
                SET status = 'uploaded',
                    s3_path = :s3_path
                WHERE id = :id
                """
            ),
            {"id": entry_id, "s3_path": s3_path},
        )
        db.commit()

        return "uploaded"
    finally:
        db.close()


__all__ = [
    "get_ingestion",
    "convert_to_csv_and_mark_converted",
    "upload_csv_to_s3_and_mark_uploaded",
]

