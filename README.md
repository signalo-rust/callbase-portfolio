# callbase — contact-data management at scale

A system for turning huge, messy contact databases into clean, filtered, segmented, exportable lists.
The interesting engineering here is all in the **data layer**: importing hundreds of thousands of rows
fast, enriching them without hammering an external API, and querying them under real load without the
database falling over.

Built as a solo project. Think of it as an ETL pipeline with a UI on top: **import → dedupe → enrich →
filter/segment → export.**

> 📐 **[architecture/](./architecture)** — pipeline and data-layer diagrams (render on GitHub).
> 💻 **[sanitized-code/](./sanitized-code)** — real extracts: the enrichment worker (advisory lock,
> keyset pagination, rate limiting) and the batched two-level import dedup.
> This is a **case-study repository** — the production source is private (real client data); what's public
> is the architecture, the reasoning, and sanitized code.

---

## The problem it solves

Raw contact bases are useless as-is: duplicates everywhere, phone numbers in ten different formats, no
idea which region or carrier a number belongs to, no way to slice out the segment you actually want. A
CSV with 300,000 rows is a liability until something cleans it. callbase is that something.

---

## The pipeline

### 1. Import — built for volume
CSVs are uploaded to object storage, then a background worker ingests them:

- **Phone normalization** handles the real-world mess — separators, Excel's scientific-notation
  mangling, the `.0` it tacks onto numbers, Russian `8→7` prefixes, 10-digit numbers → `+7`. Each row is
  classified: accepted, foreign, invalid, or empty.
- **Batched writes.** Rows go in 1,000 at a time using a single bulk insert per table per batch, instead
  of one insert per row. Three tables get written per flush (the raw rows, the canonical contacts, and
  the many-to-many link between contacts and their source base).
- **Dedup at two levels.** Duplicates *within the file* are caught with an in-memory set; duplicates
  *against everything already in the database* are caught with `INSERT … ON CONFLICT (phone) DO NOTHING
  RETURNING` — the rows that come back are the genuinely new ones, so the global-duplicate count falls
  out of the difference. No second scan needed.
- **Auto-ANALYZE after bulk writes** so the query planner doesn't go stale after a big import.

### 2. Enrich — polite to the external API, fast for us
Each number gets a region and carrier looked up from an external service. Doing that for hundreds of
thousands of numbers without getting rate-limited or taking all week is the whole challenge:

- **Rate limiting** with a token-spacing approach on a monotonic clock (configurable requests-per-second),
  plus a thread pool for concurrency — fast, but never over the API's limit.
- **Retries with exponential backoff** on 429s and 5xx.
- **Keyset (cursor) pagination** over the un-enriched rows — `WHERE … > (last cursor) ORDER BY …` backed
  by a partial index — so it never uses a slow `OFFSET` and never re-scans what it already did.
- **Safe concurrency.** A Postgres advisory lock guarantees only one enrichment job runs at a time;
  `FOR UPDATE SKIP LOCKED` hands out jobs cleanly; a heartbeat detects a dead job and re-queues it.
- Writes only fill empty fields, and newly-seen regions/carriers get added to the dictionary tables as it
  goes.

### 3. Filter, segment, export
- **Filtering** by search, source base, usage/recency windows, region, carrier, and — importantly —
  **usage-based exclusion** so the same contact isn't pulled into overlapping campaigns.
- **A fast path** for the common case: filtering a single base with no other conditions reads a
  pre-computed count instead of a full `COUNT(*)`, under a tiered statement timeout so a heavy query can't
  hang the system.
- **Overlap analytics** between bases are pre-computed and cached, so "how much do these two lists
  overlap" is instant instead of a cross-join every time.
- **Export** streams CSV in-memory in two modes (phones only, or full contact), Excel-friendly (BOM +
  delimiter hint).

*(A telephony integration sits on top of this — dispatching selected segments to a calling provider — but
the engineering that matters is everything above: getting clean, deduped, enriched data out the other
end.)*

---

## How it's built

- **Backend:** Python + FastAPI, raw SQL via psycopg2 — **no ORM**, on purpose, because the whole point
  is tight control over how the queries run at scale.
- **Workers:** lean standalone Python processes under a supervisor that runs them as isolated child
  processes with graceful shutdown and crash recovery.
- **Frontend:** Next.js + React + Tailwind — a thin client over the API.
- **Database:** PostgreSQL with `pg_trgm` for fuzzy search. **72 SQL migrations, ~62 tables, 176 indexes**
  (17 of them GIN trigram indexes for fast search on names and phones), many added iteratively as
  dedicated performance passes.

## The engineering worth pointing at

- **A hand-rolled connection pool** with a bounded wait on exhaustion, a liveness check (`SELECT 1`) on
  every checkout, and automatic discard-and-recreate of dead connections. This is the difference between
  "works in dev" and "survives a real load spike."
- **Everything volume-sensitive is a config knob** — ~30 environment settings for batch sizes, requests-
  per-second, pool sizing, query timeouts. You tune throughput without touching code.
- **Pre-computed stat tables** (base sizes, overlaps, per-contact usage) so heavy analytics don't scan
  the whole table at query time.
- **Concurrency done right** with advisory locks, skip-locked job queues, and heartbeat-based recovery of
  stuck jobs.

## Scale, honestly

72 migrations · ~62 tables · 176 indexes · import at 1,000 rows/batch · enrichment at a configurable
rate with retry and cursor pagination. The commit history is mostly sustained performance hardening —
"optimize contact history filters," "harden the fast path" — which is what building for scale actually
looks like.

---

*Built by Rustem Idiiatullin. Data engineering — import, enrich, filter, export at volume. Relocating to
Australia or New Zealand.*
