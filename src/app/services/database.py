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
        db.commit()
    finally:
        db.close()