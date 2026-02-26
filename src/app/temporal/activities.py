import json
import os
from pathlib import Path

from sqlalchemy import text
from temporalio import activity

from services.database import SessionLocal
from services.s3 import upload_csv, download_csv
from services.temporal import process_csv_file as _process_csv_file

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


@activity.defn
async def process_csv_file(s3path: str) -> None:
    await _process_csv_file(s3path)


@activity.defn
async def ingest_csv_from_s3(s3path: str) -> str:
    local_path = download_csv(s3path)
    db = SessionLocal()
    try:
        import csv as csv_mod

        with open(local_path, newline="", encoding="utf-8") as f:
            reader = csv_mod.DictReader(f)
            for row in reader:
                # Check if patient already exists
                existing = db.execute(
                    text("SELECT id FROM patients WHERE mrn = :mrn"),
                    {"mrn": row["mrn"]},
                ).scalar()

                if existing is not None:
                    patient_id = existing
                    db.execute(
                        text(
                            """
                            INSERT INTO persons (id, first_name, last_name, birth_date)
                            VALUES (:id, :first_name, :last_name, :birth_date)
                            ON CONFLICT (id) DO UPDATE SET
                                first_name = COALESCE(EXCLUDED.first_name, persons.first_name),
                                last_name = COALESCE(EXCLUDED.last_name, persons.last_name),
                                birth_date = COALESCE(EXCLUDED.birth_date, persons.birth_date)
                            """
                        ),
                        {
                            "id": patient_id,
                            "first_name": row["first_name"] or None,
                            "last_name": row["last_name"] or None,
                            "birth_date": row["birth_date"] or None,
                        },
                    )
                else:
                    # New patient: get next id from sequence
                    patient_id = db.execute(
                        text("SELECT nextval('patients_id_seq')")
                    ).scalar()
                    # Insert person first (FK: patients.id → persons.id)
                    db.execute(
                        text(
                            """
                            INSERT INTO persons (id, first_name, last_name, birth_date)
                            VALUES (:id, :first_name, :last_name, :birth_date)
                            """
                        ),
                        {
                            "id": patient_id,
                            "first_name": row["first_name"] or None,
                            "last_name": row["last_name"] or None,
                            "birth_date": row["birth_date"] or None,
                        },
                    )
                    # Insert patient with the same id
                    db.execute(
                        text(
                            "INSERT INTO patients (id, mrn) VALUES (:id, :mrn)"
                        ),
                        {"id": patient_id, "mrn": row["mrn"]},
                    )

                # Insert visit (skip duplicates)
                db.execute(
                    text(
                        """
                        INSERT INTO visits (visit_account_number, patient_id, visit_date, reason)
                        VALUES (:visit_account_number, :patient_id, :visit_date, :reason)
                        ON CONFLICT (visit_account_number) DO NOTHING
                        """
                    ),
                    {
                        "visit_account_number": row["visit_account_number"],
                        "patient_id": patient_id,
                        "visit_date": row["visit_date"],
                        "reason": row["reason"],
                    },
                )

        db.commit()
        return "ingested"
    finally:
        db.close()
        os.unlink(local_path)


__all__ = [
    "get_ingestion",
    "convert_to_csv_and_mark_converted",
    "upload_csv_to_s3_and_mark_uploaded",
    "process_csv_file",
    "ingest_csv_from_s3",
]

