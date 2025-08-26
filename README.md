### Job Process Service
A lightweight batch worker that ingests customer feedback from a database, performs NLP analysis (sentiment + zero‑shot topic classification), computes word counts, and indexes results into OpenSearch for search/analytics. The worker maintains job state in the database and runs on a simple schedule.


### High-level architecture
- **Config (`config.py`)**: Loads configuration from environment variables, and in non-local environments also pulls secrets from AWS Secrets Manager. Keys include database and OpenSearch credentials.
- **Logging (`logger.py`)**: JSON-structured logger wrapper around Python logging for consistent, machine-parsable logs.
- **Database bootstrap (`database/config.py`)**: SQLAlchemy engine/session factory and `Base` declaration plus `get_db()` generator for scoped sessions.
- **ORM models (`models/*`)**:
  - `models/user.py` — simple `User` entity (auth not implemented here).
  - `models/feedback.py` — incoming feedback items with text, product name, sender, media URLs, and timestamps.
  - `models/topics.py` — list of topic labels with optional description; only `is_active=True` are used for classification.
  - `models/job.py` — job tracking with `JobStatus` enum (`processing|completed|failed`) and `last_processed_id` checkpointing.
  - `models/job_config.py` — singleton-like key to hold a JSON config (e.g., `batch_size`, `run_next_job`).
- **Worker (`process.py`)**: Main processing loop. Schedules recurring runs, loads ML pipelines, fetches batches of new feedbacks, infers sentiment and topics, computes word counts, indexes to OpenSearch, and updates job state.
- **Operational scripts**:
  - `resume_job.py` — sets `run_next_job=True` in `JobConfig`.
  - `stop_job.py` — sets `run_next_job=False` in `JobConfig`.
  - `job_status.py` — exit code utility to check for any non-completed jobs.


### Architecture

This repository is a standalone batch Feedback Analyzer worker. It periodically reads new feedbacks from PostgreSQL, enriches them with NLP (sentiment + topics), optionally indexes results to OpenSearch, and updates job state in the database.

```mermaid
flowchart LR
  Scheduler["schedule (every 10s)"] --> RunJob["FeedbackWorker.run_job() (process.py)"]

  subgraph Config[config.py]
    Env["Environment variables / .env"]
    Secrets["AWS Secrets Manager (non-local)"]
  end
  Env --> Cfg["Runtime config"]
  Secrets -.-> Cfg
  Cfg --> DB[(PostgreSQL)]
  Cfg -->|if present| OS[(OpenSearch)]

  subgraph DB
    F[(feedbacks)]
    T[(topics)]
    J[(jobs)]
    JC[(job_configs)]
  end

  RunJob --> Guard["Check JobConfig.run_next_job"]
  Guard -->|false| Skip["Skip run"]
  Guard -->|true| ReadBatch["Read next batch > last_processed_id"]
  ReadBatch --> F
  RunJob --> LoadTopics["Load active topics"]
  LoadTopics --> T

  RunJob --> Pipelines["Hugging Face pipelines\n- sentiment-analysis\n- zero-shot-classification"]
  RunJob --> WordCount["Word count (stopwords filtered)"]
  Pipelines --> Results["Enriched feedbacks (sentiment, topics)"]

  Results -->|optional| OSFA[(feedback-analysis index)]
  WordCount -->|optional| OSWC[(wordcount-analysis doc: accumulated_word_counts)]

  RunJob --> UpdateJob["Mark Job COMPLETED; update last_processed_id"]
  UpdateJob --> J
  JC -->|run_next_job, batch_size| RunJob
```


### Data flow
1. Worker starts and initializes:
   - A local Dask cluster (1 worker, 1 thread) for model inference orchestration.
   - Hugging Face pipelines:
     - Sentiment: `distilbert-base-uncased-finetuned-sst-2-english`
     - Zero-shot topic classification: `facebook/bart-large-mnli`
   - Optional OpenSearch client if endpoint/credentials are present.
2. Topics are loaded from DB (`Topic` where `is_active=True`).
3. Job control:
   - Reads `JobConfig` (creates default if missing).
   - Skips run if `run_next_job=False`.
   - Determines `last_processed_id` from most recent `Job` and decides whether to create a new `Job` or reuse the last.
4. Fetch next batch of feedbacks where `id > last_processed_id` (default `batch_size=10`).
5. Parallel processing (thread pool):
   - Sentiment + topic classification per feedback (topic threshold 0.7, up to top 3 labels; `__UNKNOWN__` if no text).
   - Word-count aggregation over the batch (stopword-filtered, min length 3).
6. Optional indexing (if OpenSearch configured):
   - Feedback analysis to index `feedback-analysis` (one doc per feedback).
   - Word counts to index `wordcount-analysis` (upserts to a single doc `accumulated_word_counts`).
7. Persist job result: mark `Job` as `COMPLETED`, update `last_processed_id`, log resource metrics, and perform cleanup.
8. Loop: scheduled every second.


### Files overview
- `config.py` — Loads env config; if `FLASK_ENV != local`, augments via AWS Secrets Manager. Keys used:
  - `DATABASE_URL` (required), `OPENSEARCH_ENDPOINT`, `OPENSEARCH_USER`, `OPENSEARCH_PASS`
  - Other keys present for future extensions: `OPENAI_API_KEY`, `TWILIO_*`, `JWT_SECRET_KEY`, etc.
- `logger.py` — `get_logger(name)` returns a logger with `info/warning/error/debug` methods that emit JSON.
- `database/config.py` — Creates SQLAlchemy engine from `DATABASE_URL`, defines `SessionLocal`, `Base`, and `get_db()` generator.
- `models/user.py` — `User` table.
- `models/feedback.py` — `Feedback` table with `media_urls` JSON column.
- `models/topics.py` — `Topic` table with `label`, `description`, `is_active`.
- `models/job.py` — `JobStatus` enum and `Job` table with `job_name`, `last_processed_id`, status, timestamps.
- `models/job_config.py` — `JobConfig` table with JSON `config` field.
- `process.py` — Worker implementation and scheduling entrypoint (`python process.py`).
- `resume_job.py` — CLI to enable the worker via DB flag.
- `stop_job.py` — CLI to disable the worker via DB flag.
- `job_status.py` — CLI returning exit codes to indicate presence of incomplete jobs.
- `requirements.txt` — Pinned dependencies for runtime.


### Setup
1. Python 3.11 recommended. Create and activate a virtualenv.
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Environment variables:
   - Required:
     - `DATABASE_URL` — e.g. `postgresql+psycopg2://user:pass@host:5432/dbname`
   - Optional (enable OpenSearch indexing):
     - `OPENSEARCH_ENDPOINT` — e.g. `https://your-opensearch-domain:443`
     - `OPENSEARCH_USER`
     - `OPENSEARCH_PASS`
   - Optional (secrets / future features): `OPENAI_API_KEY`, `TWILIO_ACCOUNT_SID`, `TWILIO_AUTH_TOKEN`, `TWILIO_WHATSAPP_FROM`, `JWT_SECRET_KEY`.
   - Local vs prod:
     - Set `FLASK_ENV=local` to skip AWS Secrets Manager and use your local `.env`/environment.
4. Database schema:
   - This project uses SQLAlchemy models. Use your existing migration workflow (e.g., Alembic) to create tables from the models in `models/`.


### Running
- Start the worker (scheduled every 10s):
  ```bash
  python process.py
  ```
- Stop future batch runs (set `run_next_job=False`):
  ```bash
  python stop_job.py
  ```
- Resume future batch runs (set `run_next_job=True`):
  ```bash
  python resume_job.py
  ```
- Check if any job is not completed (exit codes):
  ```bash
  python job_status.py; echo $?
  ```
  - 1 → An incomplete job exists
  - 0 → No incomplete job (or DB connection could not be established)
  - 2 → Query error while checking


### Notes & behaviors
- NLTK stopwords are auto-downloaded on first run if missing.
- Topic classification keeps labels above 0.7 score (up to top 3). If none, the top label is used as fallback.
- Memory and disk usage is logged after each batch for observability.
- OpenSearch is optional; when configured, feedback and word-count indices default to:
  - `feedback-analysis`
  - `wordcount-analysis` (document id: `accumulated_word_counts`)
- `JobConfig.config` keys used:
  - `batch_size` (default 10)
  - `run_next_job` (True/False)
  - Additional keys like `num_workers` may exist for future scaling.
