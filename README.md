# Mini Distributed Cloud Object Store

A fully working distributed object storage system built with Python (FastAPI) and Docker.

---

## Architecture

```
Client(s)
   │  REST API
   ▼
┌─────────────────────────────────────┐
│          Controller  :8000          │
│  - Client REST API                  │
│  - Chunk routing & metadata         │
│  - Replication health monitor       │
│  - Worker registry + heartbeat      │
└────────┬────────────────────────────┘
         │  Internal HTTP API
    ┌────┼────┬────┬────┐
    ▼    ▼    ▼    ▼    ▼
 W1:8001 W2:8002 W3:8003 W4:8004
 (each with /data/chunks volume)
```

| Parameter | Value |
|---|---|
| Controller nodes | 1 |
| Worker/Storage nodes | 4 |
| Chunk size | 1 MB |
| Replication factor | 2 |
| Deployment | Docker / Docker Compose |
| Worker storage | Persistent named volumes |

---

## Project Structure

```text
cloud-project/
├── controller/             # Controller FastAPI node & Docker config
├── deploy/                 # Deployment configurations
├── docs/                   # Project documentation
├── scripts/                # Utility & helper scripts
├── shared/                 # Shared schemas, constants & utilities
├── tests/                  # Automated test suite
│   ├── data_integrity/
│   ├── download/
│   ├── failure_recovery/
│   ├── integration/
│   ├── replication/
│   ├── upload/
│   ├── conftest.py
│   ├── requirements-test.txt
│   └── test_controller.py
├── worker/                 # Storage worker node & Docker config
│   ├── app/
│   ├── tests/
│   ├── Dockerfile
│   ├── main.py
│   └── requirements.txt
├── client.py               # CLI client
├── demo.py                 # Full end-to-end demonstration script
├── docker-compose.yml      # Multi-container orchestration (1 controller + 4 workers)
├── pytest.ini              # Pytest configuration
└── README.md
```
---

## Quick Start

### Prerequisites
- Docker Desktop (or Docker Engine + Docker Compose v2)
- Python 3.9+ (for running `client.py` / `demo.py` from the host)

### 1. Build and Start

```bash
cd cloud-project
docker compose up --build -d
```

This starts:
- 1 × controller on `localhost:8000`
- 4 × workers on `localhost:8001` – `localhost:8004`

Wait ~10 seconds for all workers to register with the controller.

### 2. Verify the system is healthy

```bash
curl http://localhost:8000/health
```

You should see 4 workers listed as `"alive": true`.

### 3. Install client dependencies (host machine)

```bash
pip install requests
```

---

## Using the Client

```bash
# Upload a file
python client.py upload myfile.pdf

# Upload with custom metadata
python client.py upload myfile.pdf --metadata '{"author":"Alice","tag":"important"}'

# List all objects
python client.py list

# Download an object
python client.py download <object_id>

# Download and save to specific path
python client.py download <object_id> --out output.pdf

# Get metadata only (no data download)
python client.py metadata <object_id>

# Update metadata (no re-upload required)
python client.py update-metadata <object_id> '{"status":"reviewed","rating":"5"}'

# Delete an object
python client.py delete <object_id>

# System health (workers status)
python client.py health
```

Set `CONTROLLER_URL` to override the default `http://localhost:8000`:

```bash
CONTROLLER_URL=http://localhost:8000 python client.py health
```

---

## REST API Reference

### Client-Facing API

| Method | Endpoint | Purpose | Success |
|--------|----------|---------|---------|
| `POST` | `/api/v1/objects` | Upload an object | 201 |
| `GET` | `/api/v1/objects/{object_id}` | Download an object | 200 |
| `GET` | `/api/v1/objects/{object_id}/metadata` | Retrieve metadata | 200 |
| `PATCH` | `/api/v1/objects/{object_id}/metadata` | Update custom metadata | 200 |
| `DELETE` | `/api/v1/objects/{object_id}` | Delete an object | 204 |
| `GET` | `/api/v1/objects` | List objects (paginated) | 200 |
| `GET` | `/health` | System health | 200 |

#### Upload — `POST /api/v1/objects`

```
Content-Type: multipart/form-data
Fields:
  file     (required) — binary file data
  metadata (optional) — JSON string of custom key-value pairs
```

```bash
curl -X POST http://localhost:8000/api/v1/objects \
  -F "file=@/path/to/myfile.pdf" \
  -F 'metadata={"author":"Bob","project":"demo"}'
```

Response `201`:
```json
{
  "object_id": "uuid-...",
  "name": "myfile.pdf",
  "size": 2097152,
  "content_type": "application/pdf",
  "num_chunks": 2,
  "checksum": "sha256hex...",
  "created_at": "2026-01-01T12:00:00+00:00"
}
```

#### Download — `GET /api/v1/objects/{object_id}`

```bash
curl -O http://localhost:8000/api/v1/objects/<object_id>
```

Returns the original binary with correct `Content-Type` and `Content-Disposition` headers.

#### List — `GET /api/v1/objects?limit=100&offset=0`

```json
{
  "total": 5,
  "limit": 100,
  "offset": 0,
  "objects": [
    {
      "object_id": "...",
      "name": "myfile.pdf",
      "size": 2097152,
      "content_type": "application/pdf",
      "created_at": "...",
      "last_modified": "..."
    }
  ]
}
```

#### Metadata — `GET /api/v1/objects/{object_id}/metadata`

Returns full metadata including chunk locations and custom metadata.

#### Update Metadata — `PATCH /api/v1/objects/{object_id}/metadata`

```bash
curl -X PATCH http://localhost:8000/api/v1/objects/<object_id>/metadata \
  -H "Content-Type: application/json" \
  -d '{"status": "archived", "reviewed": "true"}'
```

#### Delete — `DELETE /api/v1/objects/{object_id}`

Returns `204 No Content`. Chunk cleanup happens in the background.

---

### Internal Worker API

| Method | Endpoint | Purpose |
|--------|----------|---------|
| `POST` | `/internal/workers/register` | Register worker with controller |
| `POST` | `/internal/workers/heartbeat` | Send heartbeat |
| `PUT` | `/internal/chunks/{chunk_id}` | Store a chunk |
| `GET` | `/internal/chunks/{chunk_id}` | Retrieve a chunk |
| `DELETE` | `/internal/chunks/{chunk_id}` | Delete a chunk |
| `GET` | `/internal/workers/{worker_id}/chunks` | List chunks on a worker |

---

## Running the Full Demonstration

```bash
python demo.py
```

This runs all 6 required demonstration scenarios automatically:

1. **Object Storage** — Upload 512 KB file, download it, verify SHA-256 matches
2. **Chunking** — Upload 3.5 MB file, display chunk-to-worker distribution table
3. **Replication** — Verify every chunk has exactly 2 copies on different workers
4. **Metadata** — Upload with custom metadata, retrieve, update without re-uploading
5. **Worker Failure** — Stop `worker2`, prove retrieval still works, restart it, verify re-replication
6. **Data Integrity** — Corrupt a chunk byte-by-byte, show controller detects + skips it

> **Note for Step 5 (Worker Failure):** The demo script calls `docker stop worker2` / `docker start worker2`.  
> Run `demo.py` from your **host machine** (not inside Docker) so it has access to the Docker socket.

---

## Worker Failure Handling

The controller detects a dead worker when its heartbeat has not been received for **15 seconds** (configurable). Every **10 seconds**, a background task:

1. Scans all stored chunks
2. Identifies under-replicated chunks (fewer than 2 live replicas)
3. Fetches data from a surviving replica
4. Re-replicates to another alive worker

```bash
# Manual demo
docker stop worker2
# wait ~20 seconds
curl http://localhost:8000/health                  # worker2 shows dead
curl http://localhost:8000/api/v1/objects/<id>     # still works!

docker start worker2
# wait ~20 seconds for re-registration and re-replication
curl http://localhost:8000/health                  # worker2 alive again
curl http://localhost:8000/api/v1/objects/<id>/metadata  # chunks restored
```

---

## Data Integrity

Every chunk has a **SHA-256 checksum** stored alongside its metadata on the controller.  
On download, the controller:

1. Fetches a chunk from a replica
2. Computes `sha256(data)` and compares to stored checksum
3. If corrupt → logs a warning, tries the **next replica**
4. Returns `503` only if **all replicas** are unreadable or corrupted

```bash
# Manual corruption test
curl http://localhost:8001/internal/chunks/<chunk_id>  -o chunk.bin
# overwrite with garbage:
python -c "open('chunk.bin','wb').write(b'CORRUPTED'*1000)"
curl -X PUT http://localhost:8001/internal/chunks/<chunk_id> \
     -H "Content-Type: application/octet-stream" \
     --data-binary @chunk.bin
# Download still works using the healthy replica on another worker:
curl http://localhost:8000/api/v1/objects/<object_id> -o recovered.bin
```

---

## Stopping / Cleanup

```bash
# Stop all containers
docker compose down

# Stop and remove all data volumes (full reset)
docker compose down -v
```

---

## Error Codes

| Status | Usage |
|--------|-------|
| 200 | Successful request |
| 201 | Object created |
| 204 | Successful deletion |
| 400 | Invalid request / bad JSON |
| 404 | Object or chunk not found |
| 409 | Conflict |
| 500 | Internal server error |
| 503 | Insufficient workers / chunk unavailable |

All error responses include a JSON body: `{"error": "description"}`.
