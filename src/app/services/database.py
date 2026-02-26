from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import os


DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg2://postgres:password@db:5432/db",
)

engine = create_engine(DATABASE_URL, echo=False, future=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

async def initialize():
    db = SessionLocal()
    try:
        db.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS ingestions (
                    id SERIAL PRIMARY KEY,
                    payload JSONB NOT NULL,
                    md5_hash TEXT NOT NULL UNIQUE,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    status TEXT NOT NULL DEFAULT 'new',
                    csv_filename TEXT,
                    s3_path TEXT
                )
                """
            )
        )

        db.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS patients (
                    id SERIAL PRIMARY KEY,
                    mrn VARCHAR(200) NOT NULL UNIQUE,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
        )

        db.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS persons (
                    id INT NOT NULL PRIMARY KEY,
                    first_name TEXT,
                    last_name TEXT,
                    birth_date DATE
                )
                """
            )
        )

        db.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS visits (
                    id SERIAL PRIMARY KEY,
                    visit_account_number VARCHAR(300) UNIQUE,
                    patient_id INT NOT NULL,
                    visit_date DATE NOT NULL,
                    reason TEXT
                )
                """
            )
        )

        db.execute(
            text(
                """
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM pg_constraint WHERE conname = 'person_entry'
                    ) THEN
                        ALTER TABLE public.patients
                            ADD CONSTRAINT person_entry FOREIGN KEY (id)
                                REFERENCES public.persons (id) MATCH SIMPLE
                                ON UPDATE CASCADE
                                ON DELETE CASCADE
                                NOT VALID;
                    END IF;
                END
                $$;
                """
            )
        )

        db.execute(
            text(
                """
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM pg_constraint WHERE conname = 'patient_entry'
                    ) THEN
                        ALTER TABLE public.visits
                            ADD CONSTRAINT patient_entry FOREIGN KEY (patient_id)
                                REFERENCES public.patients (id) MATCH SIMPLE
                                ON UPDATE CASCADE
                                ON DELETE CASCADE
                                NOT VALID;
                    END IF;
                END
                $$;
                """
            )
        )

        db.commit()
    finally:
        db.close()