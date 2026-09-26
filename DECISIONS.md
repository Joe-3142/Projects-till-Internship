# Decisions Log

A running record of the engineering decisions made on this project, and why.
The README says what this is; this file says why it's built this way — kept
separate on purpose, because rationale rots faster than architecture and
deserves its own place to get updated.

Each entry: **Context** → **Decision** → **Consequences**. Where a decision is
the kind that gets probed in a design review, I've added **In a review** —
what a senior engineer would actually push back on, and how I'd defend it.
New entries get appended at the bottom, oldest first — this file is a history
of how the pipeline evolved, not just its current state.

---

### 001 — Don't trust `id` as a key; hash the row content instead

**Context:** `trx_10k` has 10,000 rows but only 99 distinct `id` values — each
reused roughly 100 times across otherwise-unrelated transactions (different
time, city, amount, status). Not a duplication bug. `id` here just isn't a
transaction identifier — more likely a leftover account/customer reference
that was never meant to be unique per row.

**Decision:** `transaction_key` = SHA-256 hash of each row's actual business
content (`source_system`, `id`, `status`, `card_type`, `city`,
`transaction_date`, `hour`, `minute`, `second`, `amount`). This is the grain
key for silver and gold. The original `id` survives as `source_id` — audit
only, never a join key.

**Consequences:** Two rows identical on every business column collapse into
one key — correct, that's what a genuine duplicate is. An `id` collision
between two otherwise-different rows is correctly treated as two distinct
events.

**In a review:** The obvious pushback is "what if two genuinely different
transactions hash to the same key?" Answer: given time resolved to the second
plus exact amount, the collision space is small enough to accept for this
dataset's volume — but I'd want that stated as an assumption, not left
implicit. The stronger pushback, the one I'd actually expect from a staff
engineer: *why didn't you go back to the source system and ask what `id` is
actually supposed to mean?* A content hash is the right engineering answer
when you can't get a better source key. It's still a workaround, not a fix,
and a real production incident review would flag that upstream question as
unresolved, not solved.

---

### 002 — One conformed silver table, built from a config loop, not four hand-copied tables

**Context:** The original approach processed each of the four sources in its
own notebook cell block with copy-pasted cleaning logic. A fifth source meant
copying an entire cell. Worse: a fix applied to one source silently never
reached the other three — the kind of drift that doesn't show up until an
audit.

**Decision:** All per-source differences (bronze table, time format) live in
a `SOURCE_CONFIGS` list; every source runs through the same
`transform_source()` function; all four outputs `unionByName` into one
`silver.slv_transactions`.

**Consequences:** A 5th source costs one config entry. A cleaning fix
propagates to every source automatically on the next run.

**In a review:** This is the "looks good and actually scales" answer, not
just the "looks good" one — but only as long as sources genuinely differ by
*configuration* (time format, table name), not by *logic*. The moment a
future source needs a fundamentally different cleaning step (say, amounts in
a different currency needing conversion), this pattern needs a real extension
point — a per-source hook function, not another `if` branch bolted onto
`transform_source`. Worth saying that limit out loud before someone else hits
it and "fixes" it by re-introducing per-source cell copies.

---

### 003 — Cleaning maps degrade gracefully instead of enforcing a closed allowlist

**Context:** An earlier version checked city values against a fixed "known
cities" allowlist (Tehran, Isfahan, Tabriz) and rejected anything else — which
broke on every legitimate city not on that list. Karaj, Sanandaj, and others
were being thrown out as invalid data when they were just... cities.

**Decision:** `STATUS_MAP` / `CARD_TYPE_MAP` / `CITY_MAP` map *known
misspellings and variants only* (`"tehr@n" → "Tehran"`). Anything not in the
map passes through trimmed and title-cased instead of getting rejected.

**Consequences:** New legitimate values are never wrongly rejected. Trade-off
runs the other way now: a genuine typo not yet in the map slips through as
its own "clean" value (`"Tehrn"` stays `"Tehrn"`) until someone notices it
sitting in `dim_location` and adds it to the map.

**In a review:** This is the correct trade-off, and I'd defend it without
hesitation — false negatives (missed cleanup, visible and fixable later) beat
false positives (silently discarding real data) every time. What I'd want
paired with it, and what's currently missing: a scheduled or triggered query
against `dim_location`/`dim_card`/`dim_status` looking for suspiciously
similar values (fuzzy match, edit distance) that *should* be merged. Right
now the map only grows when a human happens to spot a problem by eye. That's
fine at this data volume; it's not a strategy, it's a stopgap.

---

### 004 — Every row lands in `silver` or `silver_rejected`, with a reason, for all sources

**Context:** Silently dropping or corrupting invalid rows on some sources but
not others produces a pipeline that *looks* validated but isn't — arguably
worse than no validation at all, because the gap is invisible until someone
goes looking for it.

**Decision:** `flag_rejects()` runs identically across every source, checking
for invalid/unparseable time, missing amount, out-of-range amount, missing
id. Failures land in `silver.slv_transactions_rejected` with a
`reject_reason`. Nothing is silently dropped.

**Consequences:** Row counts are always reconcilable — clean + rejected =
input. Reject reasons are queryable by source, so data-quality issues are
visible instead of anecdotal.

**In a review:** This pattern is correct and I wouldn't change the shape of
it. But "correct pattern, incomplete checklist" is worth separating —
`flag_rejects()` catches rows with *bad values*. It does nothing for a
column that's *structurally absent* due to a schema mismatch (see the `fees`
issue in the open items below). A validation suite that only checks values
and never checks "did every column I expect to be populated actually get
populated, at a nonzero rate, from this source" has a blind spot. That's the
gap that let the `fees` problem ship silently.

---

### 005 — `amount` is `DECIMAL(18,2)`, not float

**Context:** `amount` gets summed across tens of thousands of rows for
aggregate reporting — financial data, where binary floating-point error
compounding at scale is a real, not theoretical, problem.

**Decision:** Cast `amount` to `DecimalType(18, 2)` in silver, and keep
`fees`/`discount`/`balance_before` Decimal too.

**Consequences:** Sums and aggregates are exact to the cent.

**In a review:** This one's not really debatable — it's the kind of decision
that's either obviously right or a red flag that the engineer hasn't worked
with financial data before. The only follow-up question I'd ask: is
`(18, 2)` precision/scale actually sized against a real max transaction
value, or picked because it's a common default? Given `MAX_PLAUSIBLE_AMOUNT`
elsewhere in this pipeline is also an unvalidated placeholder, I'd bet on the
latter — worth confirming both together.

---

### 006 — Gold reads directly from the unified silver table; the `all_silver_for_dims` union view is gone

**Context:** The original gold notebook built dimensions from an
`all_silver_for_dims` view stitching together four *divergent* silver tables
(pre-unification) via an unfinished, broken SQL string that had evidently
never successfully run.

**Decision:** Once silver is unified (decision 002), gold's dimension
builders `SELECT DISTINCT` straight from `silver.slv_transactions`.

**Consequences:** An entire broken, dead code path is gone. Gold notebook is
simpler.

**In a review:** Not an independent decision — a direct downstream
consequence of 002. Worth noting as its own entry anyway, because "this
became simpler" is itself evidence that 002 was the right call, not a
separate thing to defend.

---

### 007 — `fact_transactions` is built from all four sources, not just one

**Context:** The original `fact_transactions` build selected only from
`slv_transactions`, which — pre-unification — was actually just `trx_10k`
alone. Three of four source files never reached the star schema. The
pipeline ran clean, no errors, and quietly used a quarter of the intended
data. That's the dangerous kind of bug: the one that doesn't crash.

**Decision:** Once silver is unified, `fact_transactions` selects from the
single `slv_transactions` table, which now inherently contains all sources by
construction.

**Consequences:** All source data reaches the analytical layer.

**In a review:** This is worth being honest about in an interview, not
glossing over: it was a correctness bug in the original design, and the fix
was a side effect of a structural change, not a targeted patch. That's a
better story than pretending it was caught by inspection — "I unified the
silver layer for maintainability reasons and it also fixed a silent data-loss
bug I hadn't noticed yet" is a more credible narrative than "I noticed a bug
and fixed it," because it's true, and because it shows the value of the
structural fix independent of the bug it happened to catch.

---

### 008 — `is_weekend` on the fact table is computed from `dim_date`, not the source column

**Context:** Only `trx_extra_10k` ever populated `is_weekend` directly.
Trusting that source column on the fact table would leave `is_weekend` NULL
for roughly 75% of transactions.

**Decision:** `dim_date` computes `is_weekend` from the calendar date itself
for every date in the table; the fact table pulls it from the `dim_date`
join, not from silver.

**Consequences:** `is_weekend` is correct and populated for 100% of rows,
computed once in one place.

**In a review:** Straightforward and correct — a derived attribute that can
be computed deterministically from an existing column should never be
sourced from an unreliable upstream field. The general principle here is
worth stating explicitly for an interview: *if you can derive it, derive it;
don't trust a source to have derived it consistently for you.*

---

### 009 — Gold validation checks the joins, not just row counts per table

**Context:** The original validation cell queried tables and columns
(`dim_card_type`, `card_type_sk`) that were never created by the build cells.
It had evidently never actually executed successfully — and wouldn't have
caught join fan-out, dropped rows, or orphaned keys even if it had run.

**Decision:** `validate()` checks: (1) row-count parity between silver and
`fact_transactions`, catching fan-out or accidental `INNER JOIN` row loss;
(2) an orphan check per dimension foreign key, catching values that don't
match any dimension row; (3) a duplicate check on `transaction_key` at fact
grain.

**Consequences:** Validation exercises the actual relationships between
tables instead of checking each in isolation.

**In a review:** This is the difference between validation that exists and
validation that works. "The old validation cell technically ran, technically
printed output, and technically checked nothing real" is exactly the failure
mode that gives dashboards false confidence in production. I'd hold this up
as the single most senior decision in the whole pipeline — not because the
checks are exotic, but because it correctly identifies that a *broken* check
is worse than *no* check, since a broken check that "passes" gets trusted.

---

## Open items (flagged, not yet decided/fixed)

Known issues raised during development, not yet resolved. Logged here so
they don't get lost, and so whichever entry eventually closes each one can
reference why the fix mattered.

- **`fees` may always be NULL for `trx_extra_10k`.** Bronze source 3's schema
  names the raw column `field`; silver's `conform_columns` looks for `fees`,
  doesn't find it, defaults to NULL. Needs confirmation of whether `field`
  was meant to be `fees` before this is trusted anywhere downstream. Note
  the deeper gap this exposes: current validation checks *values within a
  column*, not *whether a column is structurally receiving data at all* — a
  name mismatch like this produces zero rejects, zero errors, zero signal.
  A next-pass improvement worth its own decision entry: a per-source,
  per-column "population rate" check as part of `validate()`, that flags any
  expected-but-optional column sitting at 0% populated for a source that's
  supposed to provide it.
- **Dimension surrogate keys (`dim_card`, `dim_location`, `dim_status`) use
  `ROW_NUMBER()` over a full `CREATE OR REPLACE` each run.** Safe today
  because the whole gold layer rebuilds from scratch every run. Becomes a
  landmine the day this moves to incremental loads — a newly-seen city can
  renumber every existing `location_id` downstream, silently invalidating
  fact rows that already point at the old numbers. Fix: durable/hashed keys,
  or a `MERGE`-based key assignment that only ever appends.
- **`MAX_PLAUSIBLE_AMOUNT` (5,000,000) is a placeholder ceiling**, not
  derived from an actual distribution/percentile analysis. Should be
  revisited once there's a clear picture of what a genuine outlier looks
  like versus a data-entry error in this dataset specifically.
