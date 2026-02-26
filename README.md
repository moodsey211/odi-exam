# ODI Exam - Data Ingestion Pipeline

A FastAPI application that ingests patient/visit data via a REST API, converts it to CSV, uploads to S3, and persists records into a PostgreSQL database using Temporal workflows.

## Architecture

```
Client --> FastAPI API --> Temporal Workflow --> S3 (LocalStack) --> PostgreSQL
```

**Services:**

| Service          | Container                    | Port  | Description                        |
|------------------|------------------------------|-------|------------------------------------|
| API              | `odi_exam_api`               | 8000  | FastAPI application                |
| Temporal Worker  | `odi_exam_temporal_worker`   | -     | Runs Temporal activities           |
| Temporal Server  | `odi_exam_temporal`          | 7233  | Workflow orchestration engine      |
| Temporal UI      | `odi_exam_temporal`          | 8233  | Temporal web dashboard             |
| PostgreSQL       | `odi_exam_db`                | 5432  | Database                           |
| LocalStack (S3)  | `odi_exam_localstack`        | 4566  | Local AWS S3 emulation             |

## Setup

### Prerequisites

- Docker and Docker Compose

### 1. Build the Docker image

```bash
make build
```

This builds the `fastapi-dev:latest` image from `docker/images/fastapi-dev`.

### 2. Start all services

```bash
make start
```

This starts all containers in detached mode: PostgreSQL, LocalStack, Temporal, the API, and the Temporal worker.

### 3. Verify services are running

```bash
docker compose ps
```

All containers should show a `running` state.

### 4. Verify the API is healthy

```bash
curl http://localhost:8000/health
# {"status":"ok"}

curl http://localhost:8000/db-check
# {"db":"connected"}
```

## Makefile Usage

| Command      | Description                                             |
|--------------|---------------------------------------------------------|
| `make build` | Build the `fastapi-dev:latest` Docker image             |
| `make start` | Start all services in detached mode                     |
| `make up`    | Start the API container with an interactive bash shell  |
| `make down`  | Stop and remove all containers                          |
| `make logs`  | Tail the API container logs                             |

## API Usage

Base URL: `http://localhost:8000`

### Health Check

```
GET /health
```

Response:
```json
{"status": "ok"}
```

### Database Check

```
GET /db-check
```

Response:
```json
{"db": "connected"}
```

### Ingest Data

```
POST /ingest
Content-Type: application/json
```

Request body (JSON array):
```json
[
  {
    "mrn": "MRN-1001",
    "first_name": "John",
    "last_name": "Doe",
    "birth_date": "1990-02-14",
    "visit_account_number": "VST-9001",
    "visit_date": "2024-11-01",
    "reason": "Annual Checkup"
  }
]
```

Response:
```json
{"id": 1, "status": "created"}
```

- Returns `"status": "existing"` if the same payload was already submitted.
- Triggers the full Temporal workflow pipeline: JSON to CSV conversion, S3 upload, and database ingestion.

### List Patients

```
GET /patients
```

Query parameters:

| Parameter    | Type   | Description                              |
|--------------|--------|------------------------------------------|
| `mrn`        | string | Filter by exact MRN match                |
| `first_name` | string | Filter by first name (case-insensitive, partial match) |
| `last_name`  | string | Filter by last name (case-insensitive, partial match)  |
| `page`       | int    | Page number (default: 1)                 |
| `page_size`  | int    | Results per page (default: 20, max: 100) |

Example:
```bash
curl "http://localhost:8000/patients?last_name=doe&page=1&page_size=10"
```

Response:
```json
{
  "patients": [
    {
      "id": 1,
      "mrn": "MRN-1001",
      "first_name": "John",
      "last_name": "Doe",
      "birth_date": "1990-02-14",
      "created_at": "2026-01-01 00:00:00+00:00",
      "visits": [
        {
          "id": 1,
          "visit_account_number": "VST-9001",
          "visit_date": "2024-11-01",
          "reason": "Annual Checkup"
        }
      ]
    }
  ],
  "page": 1,
  "page_size": 10,
  "total": 1
}
```

Each patient includes up to 10 most recent visits.

### Get Patient by ID

```
GET /patients/{id}
```

Query parameters:

| Parameter          | Type | Description                                  |
|--------------------|------|----------------------------------------------|
| `visits_page`      | int  | Visits page number (default: 1)              |
| `visits_page_size` | int  | Visits per page (default: 10, max: 100)      |

Example:
```bash
curl "http://localhost:8000/patients/1?visits_page=1&visits_page_size=5"
```

Response:
```json
{
  "id": 1,
  "mrn": "MRN-1001",
  "first_name": "John",
  "last_name": "Doe",
  "birth_date": "1990-02-14",
  "created_at": "2026-01-01 00:00:00+00:00",
  "visits": [
    {
      "id": 1,
      "visit_account_number": "VST-9001",
      "visit_date": "2024-11-01",
      "reason": "Annual Checkup"
    }
  ],
  "visits_page": 1,
  "visits_page_size": 5,
  "visits_total": 1
}
```

Returns `404` if the patient does not exist.

## Postman Collection

A Postman collection is included at `DataIngestion.postman_collection.json`.

### Import

1. Open Postman and click **Import**.
2. Select the `DataIngestion.postman_collection.json` file.

### Configure Variables

After importing, set the collection variables:

| Variable | Value                    |
|----------|--------------------------|
| `url`    | `http://localhost:8000`  |
| `id`     | A valid patient ID (e.g. `1`) |

### Included Requests

| Request           | Method | Endpoint          |
|-------------------|--------|-------------------|
| /health           | GET    | `{{url}}/health`  |
| /db-check         | GET    | `{{url}}/db-check`|
| /ingest           | POST   | `{{url}}/ingest`  |
| /patients         | GET    | `{{url}}/patients`|
| /patients/\<id\>  | GET    | `{{url}}/patients/{{id}}` |

## S3 Verification

After ingesting data, verify that the CSV file was uploaded to the LocalStack S3 bucket.

### List bucket contents

```bash
docker exec odi_exam_localstack awslocal s3 ls s3://csv-uploads/ingestions/
```

Expected output:
```
2026-01-01 00:00:00       123 ingestion_1.csv
```

### Download and inspect a CSV

```bash
docker exec odi_exam_localstack awslocal s3 cp s3://csv-uploads/ingestions/ingestion_1.csv /tmp/ingestion_1.csv
docker exec odi_exam_localstack cat /tmp/ingestion_1.csv
```

Expected output:
```
mrn,first_name,last_name,birth_date,visit_account_number,visit_date,reason
MRN-1001,John,Doe,1990-02-14,VST-9001,2024-11-01,Annual Checkup
```

### Verify bucket exists

```bash
docker exec odi_exam_localstack awslocal s3 ls
```

Should include `csv-uploads`.

## Workflow Execution Validation

### Temporal Web UI

Open `http://localhost:8233` in a browser to view workflow executions, statuses, and activity history.

### List workflows via CLI

```bash
docker exec odi_exam_temporal temporal workflow list --namespace default
```

### Check a specific workflow

Each ingestion triggers two workflows:

1. **CsvConversionWorkflow** (ID: `ingest-{ingestion_id}`) - Converts JSON to CSV, uploads to S3, then triggers the ingestion workflow.
2. **CsvIngestionWorkflow** (ID: MD5 hash of the S3 path) - Downloads CSV from S3 and inserts records into the database.

To describe a workflow:
```bash
docker exec odi_exam_temporal temporal workflow describe --workflow-id ingest-1 --namespace default
```

### Verify worker is running

```bash
docker logs odi_exam_temporal_worker --tail 5
```

Should show:
```
Temporal worker started on 'background-task-queue'
```

## Database Verification

### Connect to the database

```bash
docker exec -it odi_exam_db psql -U pguser -d appdb
```

### Verify tables exist

```sql
\dt
```

Expected tables: `ingestions`, `patients`, `persons`, `visits`.

### Check ingestion status

```sql
SELECT id, status, csv_filename, s3_path FROM ingestions;
```

A fully processed ingestion should have `status = 'uploaded'`.

### Verify patient data

```sql
SELECT p.id, p.mrn, pe.first_name, pe.last_name, pe.birth_date
FROM patients p
JOIN persons pe ON pe.id = p.id;
```

### Verify visit data

```sql
SELECT v.id, v.visit_account_number, v.patient_id, v.visit_date, v.reason
FROM visits v
ORDER BY v.visit_date DESC;
```

### Verify foreign key relationships

```sql
-- Patients with their person records
SELECT p.id, p.mrn, pe.first_name, pe.last_name
FROM patients p
JOIN persons pe ON pe.id = p.id;

-- Visits linked to patients
SELECT v.visit_account_number, p.mrn, v.visit_date, v.reason
FROM visits v
JOIN patients p ON p.id = v.patient_id;
```

### Row counts

```sql
SELECT 'patients' AS table_name, COUNT(*) FROM patients
UNION ALL
SELECT 'persons', COUNT(*) FROM persons
UNION ALL
SELECT 'visits', COUNT(*) FROM visits
UNION ALL
SELECT 'ingestions', COUNT(*) FROM ingestions;
```
