from typing import Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy import text
from sqlalchemy.orm import Session

from services.database import get_db

router = APIRouter(tags=["ingestion"])

@router.get("/patients/{patient_id}")
def getPatient(
    patient_id: int,
    visits_page: int = Query(1, ge=1),
    visits_page_size: int = Query(10, ge=1, le=100),
    db: Session = Depends(get_db),
) -> dict:
    row = db.execute(
        text(
            """
            SELECT
                p.id,
                p.mrn,
                pe.first_name,
                pe.last_name,
                pe.birth_date,
                p.created_at
            FROM patients p
            LEFT JOIN persons pe ON pe.id = p.id
            WHERE p.id = :id
            """
        ),
        {"id": patient_id},
    ).mappings().first()

    if row is None:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail="Patient not found")

    visits_total = db.execute(
        text("SELECT COUNT(*) FROM visits WHERE patient_id = :patient_id"),
        {"patient_id": patient_id},
    ).scalar()

    visits_offset = (visits_page - 1) * visits_page_size

    visits = db.execute(
        text(
            """
            SELECT id, visit_account_number, visit_date, reason
            FROM visits
            WHERE patient_id = :patient_id
            ORDER BY visit_date DESC
            LIMIT :limit OFFSET :offset
            """
        ),
        {"patient_id": patient_id, "limit": visits_page_size, "offset": visits_offset},
    ).mappings().all()

    return {
        "id": row["id"],
        "mrn": row["mrn"],
        "first_name": row["first_name"],
        "last_name": row["last_name"],
        "birth_date": str(row["birth_date"]) if row["birth_date"] else None,
        "created_at": str(row["created_at"]) if row["created_at"] else None,
        "visits": [dict(v) for v in visits],
        "visits_page": visits_page,
        "visits_page_size": visits_page_size,
        "visits_total": visits_total,
    }

@router.get("/patients")
def listPatients(
    mrn: Optional[str] = Query(None),
    first_name: Optional[str] = Query(None),
    last_name: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    db: Session = Depends(get_db),
) -> dict:
    conditions = []
    params = {}

    if mrn is not None:
        conditions.append("p.mrn = :mrn")
        params["mrn"] = mrn
    if first_name is not None:
        conditions.append("pe.first_name ILIKE :first_name")
        params["first_name"] = f"%{first_name}%"
    if last_name is not None:
        conditions.append("pe.last_name ILIKE :last_name")
        params["last_name"] = f"%{last_name}%"

    where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""
    offset = (page - 1) * page_size

    total = db.execute(
        text(
            f"""
            SELECT COUNT(*) FROM patients p
            LEFT JOIN persons pe ON pe.id = p.id
            {where_clause}
            """
        ),
        params,
    ).scalar()

    params["limit"] = page_size
    params["offset"] = offset

    rows = db.execute(
        text(
            f"""
            SELECT
                p.id,
                p.mrn,
                pe.first_name,
                pe.last_name,
                pe.birth_date,
                p.created_at
            FROM patients p
            LEFT JOIN persons pe ON pe.id = p.id
            {where_clause}
            ORDER BY p.id
            LIMIT :limit OFFSET :offset
            """
        ),
        params,
    ).mappings().all()

    patients = []
    for row in rows:
        visits = db.execute(
            text(
                """
                SELECT id, visit_account_number, visit_date, reason
                FROM visits
                WHERE patient_id = :patient_id
                ORDER BY visit_date DESC
                LIMIT 10
                """
            ),
            {"patient_id": row["id"]},
        ).mappings().all()

        patients.append(
            {
                "id": row["id"],
                "mrn": row["mrn"],
                "first_name": row["first_name"],
                "last_name": row["last_name"],
                "birth_date": str(row["birth_date"]) if row["birth_date"] else None,
                "created_at": str(row["created_at"]) if row["created_at"] else None,
                "visits": [dict(v) for v in visits],
            }
        )

    return {
        "patients": patients,
        "page": page,
        "page_size": page_size,
        "total": total,
    }
