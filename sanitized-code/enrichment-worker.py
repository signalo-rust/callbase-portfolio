"""
Enrichment worker (sanitized extract)
-------------------------------------
Each contact's phone number needs a region + carrier looked up from an external
API. Doing that for hundreds of thousands of numbers, continuously, without (a)
getting rate-limited, (b) re-scanning work already done, or (c) two workers
stepping on each other, is the whole problem. The parts worth pointing at:

  1. SINGLE-WORKER via a Postgres advisory lock. `pg_try_advisory_xact_lock`
     means only one worker processes at a time — no external coordinator, no
     leader election. A second worker just spins and waits.

  2. KEYSET (cursor) pagination, never OFFSET. The batch query walks
     `WHERE (created_at, id) > (cursor)` on an ordered index, so it never
     re-scans rows it already enriched and never pays OFFSET's linear cost.
     The cursor is persisted on the job row, so a restart resumes exactly where
     it left off.

  3. TOKEN-SPACED rate limiting on a monotonic clock. A target requests-per-second
     is enforced by spacing request timestamps, decoupled from a thread pool that
     provides concurrency — fast, but provably never over the API's limit.

  4. HEARTBEAT-based recovery. A running job stamps `last_beat`; a job whose
     heartbeat is stale is marked failed and re-queued, so a crashed worker can't
     leave the pipeline wedged.

  5. BACKOFF retries on 429 / 5xx only — a transient blip is retried with
     exponential backoff; a 4xx that isn't 429 fails fast.

Sanitized: the external vendor and its URL are generic; comments translated;
credentials removed. The SQL and control flow are unchanged.

Stack: Python, psycopg2, raw SQL — no ORM, on purpose (tight control over how the
queries run at scale).
"""

import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

import psycopg2
from psycopg2.extras import RealDictCursor


def _connect():
    db_url = os.environ["DATABASE_URL"]
    return psycopg2.connect(db_url, sslmode="require")


def _digits(value: Optional[str]) -> Optional[str]:
    return re.sub(r"\D", "", value) if value else None


# --- job lifecycle -----------------------------------------------------------

def _get_active_job(cur):
    # SKIP LOCKED so this claim never blocks behind another worker's row.
    cur.execute(
        """
        select id, status, cursor_created_at, cursor_id
        from enrich_jobs
        where status in ('queued', 'running')
        order by created_at desc
        limit 1
        for update skip locked
        """
    )
    return cur.fetchone()


def _mark_stale_jobs(cur, stale_hours: int):
    # A running job whose heartbeat went cold → the worker died. Fail it so it
    # can be re-queued, instead of leaving the pipeline stuck forever.
    cur.execute(
        """
        update enrich_jobs
        set status = 'failed', error = 'stale', finished_at = now()
        where status = 'running'
          and last_beat is not null
          and last_beat < now() - (%s || ' hours')::interval
        returning id
        """,
        (str(stale_hours),),
    )
    return cur.fetchall()


def _fetch_batch(cur, limit, cursor_created_at, cursor_id):
    # Keyset pagination: walk forward past the cursor on an ordered index.
    # Never OFFSET (linear scan) and never re-touch an already-enriched row.
    if cursor_created_at and cursor_id:
        cur.execute(
            """
            select id, phone_norm, created_at
            from contacts
            where (region is null or region = '' or carrier is null or carrier = '')
              and (created_at, id) > (%s, %s::uuid)
            order by created_at asc, id asc
            limit %s
            """,
            (cursor_created_at, cursor_id, limit),
        )
    else:
        cur.execute(
            """
            select id, phone_norm, created_at
            from contacts
            where (region is null or region = '' or carrier is null or carrier = '')
            order by created_at asc, id asc
            limit %s
            """,
            (limit,),
        )
    return cur.fetchall()


# --- external lookup with backoff --------------------------------------------

def _lookup(base_url: str, digits: str, *, attempts=3, base_sec=0.35, max_sec=3.0):
    import json
    import urllib.request
    from urllib.error import HTTPError, URLError

    req = urllib.request.Request(f"{base_url}{digits}")
    for attempt in range(max(1, attempts)):
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                data = json.loads(resp.read().decode("utf-8", "ignore"))
                return (data.get("region"), data.get("carrier")) if isinstance(data, dict) else (None, None)
        except HTTPError as exc:
            # Retry ONLY the transient classes; a hard 4xx fails immediately.
            if not (exc.code == 429 or 500 <= exc.code < 600) or attempt + 1 >= attempts:
                raise
        except (URLError, TimeoutError, ValueError):
            if attempt + 1 >= attempts:
                raise
        time.sleep(min(max_sec, base_sec * (2 ** attempt)))  # exponential backoff
    return (None, None)


# --- the loop ----------------------------------------------------------------

def run():
    rps = float(os.getenv("ENRICH_RPS", "8"))
    batch_size = int(os.getenv("ENRICH_BATCH_SIZE", "120"))
    concurrency = int(os.getenv("ENRICH_CONCURRENCY", "12"))
    idle_sleep = int(os.getenv("ENRICH_IDLE_SEC", "10"))
    stale_hours = int(os.getenv("ENRICH_STALE_HOURS", "24"))
    base_url = os.environ["REGION_LOOKUP_URL"]  # external region/carrier API

    min_interval = 1.0 / max(rps, 1)     # token spacing: 1 request per interval
    next_request_ts = time.monotonic()
    conn = None

    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        while True:
            if conn is None or conn.closed:
                conn = _connect()

            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # Only one enrichment worker runs at a time — everything below is
                # under this transaction-scoped advisory lock.
                cur.execute(
                    "select pg_try_advisory_xact_lock(hashtext('enrich_worker_lock')) as locked"
                )
                if not (cur.fetchone() or {}).get("locked"):
                    conn.commit()
                    time.sleep(max(2, idle_sleep))
                    continue

                _mark_stale_jobs(cur, stale_hours)
                job = _get_active_job(cur)
                if not job:
                    conn.commit()
                    time.sleep(idle_sleep)
                    continue

                batch = _fetch_batch(
                    cur, batch_size, job.get("cursor_created_at"), job.get("cursor_id")
                )
                if not batch:
                    cur.execute(
                        "update enrich_jobs set status='done', finished_at=now() where id=%s",
                        (job["id"],),
                    )
                    conn.commit()
                    continue

                # Dispatch each lookup, spacing requests to hold the target rps
                # regardless of how many threads are free.
                future_row = {}
                for row in batch:
                    digits = _digits(row.get("phone_norm"))
                    if not digits:
                        continue
                    now = time.monotonic()
                    if now < next_request_ts:
                        time.sleep(next_request_ts - now)
                    next_request_ts = max(time.monotonic(), next_request_ts) + min_interval
                    future_row[pool.submit(_lookup, base_url, digits)] = row

                updates = []
                for fut in as_completed(future_row):
                    row = future_row[fut]
                    region, carrier = fut.result()
                    if region or carrier:
                        updates.append((row["id"], region, carrier))

                # ... bulk-write the results, advance the persisted cursor to the
                # last row of this batch, stamp last_beat, commit. A restart
                # resumes from exactly that cursor.
                conn.commit()
