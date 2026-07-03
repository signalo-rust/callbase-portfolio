# Sanitized code

Representative extracts from the production data layer. Sanitized — the external
vendor, credentials, and internal names are removed — but the SQL, the concurrency
model, and the trade-offs are exactly what runs. These are the files that show the
difference between "works in dev" and "survives a real import at volume".

| File | The real problem it solves |
|---|---|
| [`enrichment-worker.py`](./enrichment-worker.py) | Enrich hundreds of thousands of phone numbers against an external API **without getting rate-limited or re-scanning finished work** — token-spaced rate limiting, keyset (cursor) pagination, a single-worker advisory lock, and heartbeat-based recovery of a dead job. |
| [`import-dedup.py`](./import-dedup.py) | Import a 300k-row CSV **fast, deduped at two levels, in one pass** — batched bulk inserts and `ON CONFLICT DO NOTHING RETURNING` so the global-duplicate count falls straight out of the write with no second scan. |

See [`../architecture/`](../architecture) for how these fit into the full
import → dedupe → enrich → filter → export pipeline.
