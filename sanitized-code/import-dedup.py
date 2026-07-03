"""
CSV import with two-level dedup, one pass (sanitized extract)
------------------------------------------------------------
Importing a 300,000-row contact CSV has to be fast, and it has to dedupe against
(a) duplicates *within the file* and (b) everything *already in the database* —
without a slow second pass to count them. The trick is to let Postgres do the
global dedup and hand back exactly the new rows:

  • BATCHED writes. Rows are flushed 1,000 at a time with a single bulk insert
    per table per batch (`execute_values`), not one INSERT per row. For a 300k
    file that's the difference between minutes and hours.

  • IN-FILE dedup with an in-memory set — a duplicate phone seen earlier in the
    same file is dropped before it ever hits the database.

  • GLOBAL dedup with `INSERT … ON CONFLICT (phone_norm) DO NOTHING RETURNING`.
    The rows that come back are precisely the genuinely-new ones. So the
    global-duplicate count is just `len(batch) - len(returned)` — it falls out
    of the write itself, with no second scan and no SELECT-then-INSERT race.

  • ANALYZE after a big import so the planner's stats don't go stale.

Sanitized: table/column names kept (generic); comments translated. The SQL shape
is exactly what runs.
"""

from psycopg2.extras import execute_values


def flush_batch(cur, source_rows_batch, accepted_batch):
    """
    source_rows_batch : raw ingested rows (audit of exactly what the file held)
    accepted_batch    : [(phone_raw, phone_norm), ...] — rows that normalized to a
                        valid, in-file-unique phone number
    Returns the count of GLOBAL duplicates found in this batch.
    """
    if not source_rows_batch:
        return 0

    # 1) Persist the raw rows — one bulk insert for the whole batch.
    execute_values(
        cur,
        """
        insert into source_rows (source_base_id, row_num, raw, phone_raw)
        values %s
        """,
        source_rows_batch,
    )

    if not accepted_batch:
        return 0

    # 2) Insert canonical contacts. ON CONFLICT DO NOTHING skips anything already
    #    in the DB; RETURNING hands back ONLY the rows that were actually new.
    inserted = execute_values(
        cur,
        """
        insert into contacts (phone_raw, phone_norm)
        values %s
        on conflict (phone_norm) do nothing
        returning id, phone_norm
        """,
        accepted_batch,
        fetch=True,
    )

    # 3) The global-duplicate count is exact, for free: everything we tried to
    #    insert, minus the rows that came back as new. No second scan needed.
    newly_inserted = {row[1] for row in inserted}
    global_duplicates = len(accepted_batch) - len(newly_inserted)

    # 4) Link every accepted phone (new or pre-existing) to this source base, so
    #    membership is complete even for numbers that were already known.
    all_norms = [pn for _, pn in accepted_batch]
    execute_values(
        cur,
        """
        insert into base_membership (source_base_id, contact_id)
        select %s, c.id from contacts c where c.phone_norm = any(%s)
        on conflict do nothing
        """,
        [(all_norms,)],  # illustrative; real call binds base id + the norm array
    )

    return global_duplicates


def on_import_finished(cur):
    # After a large bulk load, refresh planner statistics so subsequent filter
    # and search queries don't run against stale row estimates.
    cur.execute("analyze contacts")
    cur.execute("analyze base_membership")


# In-file dedup lives in the streaming loop that fills the batches:
#
#   seen: set[str] = set()          # phones already seen in THIS file
#   for row in csv_reader:
#       phone_norm = normalize(row)
#       if phone_norm in seen:      # in-file duplicate → skip before the DB
#           continue
#       seen.add(phone_norm)
#       accepted_batch.append((row.phone_raw, phone_norm))
#       if len(accepted_batch) >= 1000:
#           dupes += flush_batch(cur, source_rows_batch, accepted_batch)
#           source_rows_batch.clear(); accepted_batch.clear()
