# Iranian Transactions — Databricks Medallion Pipeline

A data engineering portfolio project that turns four inconsistent transaction
CSV exports into a single conformed dataset and a query-ready star schema, on
Databricks with PySpark and Delta Lake.

The point of this project isn't the medallion architecture — that part's
table stakes. The point is what happens when four sources disagree with each
other about schema, time formats, and what a "duplicate" even means, and the
pipeline has to make an explicit, defensible call on each disagreement instead
of quietly averaging over it.

## The problem

Four CSV exports (`trx_10k`, `trx_99`, `trx_extra_10k`, `trx`) describe
the same kind of event — a card transaction — but don't agree on much else:

| Source | Rows | Columns beyond the basics | The catch |
|---|---|---|---|
| `trx_10k` | ~10,000 | status, time, card_type, city, amount, id | `id` has only **99 distinct values** across 10,000 rows — it's not a transaction identifier at all |
| `trx_99` | ~99 | same base columns | Different `time` string format from `trx_10k` |
| `trx_extra_10k` | ~10,000 | + fees, discount, balance_before, is_weekend | Only source with fee/discount/balance data — see Known Issues |
| `trx` | small | same base columns | Time format matches `trx_99`, not `trx_10k` |

Plus the usual: inconsistent casing (`mastercard` / `mastcard` / `master-card`),
free-text cities (`Thr`, `tehr@n`, `Tehran`), and status values needing
normalization (`succeed` → `Success`).

None of this is exotic. It's what every real ingestion pipeline looks like
before someone cleans it up. The engineering decision that matters isn't
"clean the data" — it's *how* you clean it so the next source doesn't break
everything again.

## Architecture

```
  bronze                      silver                          gold
┌──────────────┐        ┌───────────────────────┐        ┌─────────────────────┐
│ brz_trans_1  │──┐     │                       │        │  dim_date            │
│ brz_trans_2  │──┼───▶│  slv_transactions      │───┬───▶│  dim_time            │
│ brz_trans_3  │──┤     │  (one conformed table,│   │    │  dim_card            │
│ brz_trans_4  │──┘     │   one row per source   │   │    │  dim_location        │
│              │        │   record)              │   │    │  dim_status          │
│ (raw CSV,    │        │                       │   │    │                      │
│  schema-on-  │        │  slv_transactions_     │   │    │  fact_transactions   │
│  read only)  │        │  rejected              │   └───▶│  (one row per        │
└──────────────┘        │  (same shape + reason) │        │   transaction_key)   │
                         └───────────────────────┘        └─────────────────────┘
```

- **Bronze** — raw CSVs, explicit schema per source, `_source_file` +
  `ingested_at` for lineage. No cleaning. Bronze should never lie about what
  the source actually sent.
- **Silver** — every source runs through identical cleaning, validation, and
  key-generation logic via a config-driven loop, landing in **one** conformed
  table. Failures go to a parallel `_rejected` table with a reason.
- **Gold** — a standard star schema, built entirely off the unified silver
  table, with validation that checks the *joins*, not just table row counts.

## Data model (gold layer)

`fact_transactions` at the grain of one row per `transaction_key`, joined to
`dim_date`, `dim_time`, `dim_card`, `dim_location`, `dim_status`. `source_id`
(the original `id` column) rides along for audit purposes only — it is never
a join key, anywhere, on purpose.

## Engineering decisions worth defending in an interview

Full rationale log: [`DECISIONS.md`](DECISIONS.md). If someone asked me to
justify three choices in this pipeline in a design review, it'd be these:

1. **`transaction_key` is a content hash, not the source `id`.** The `id`
   column in `trx_10k` fails the most basic test of a key — uniqueness — and
   trusting it anyway would have silently merged ~9,900 distinct transactions
   into 99. This is the single decision that most separates "ran the
   pipeline" from "understood the data" on this project.
2. **One conformed silver table via config, not four hand-copied blocks.**
   The alternative (four near-identical notebook cells) is the classic
   copy-paste failure mode: a fix applied to one source silently never
   reaches the other three. A 5th source here costs one config entry.
3. **Validation checks relationships, not just row counts.** Counting rows
   per table tells you nothing about whether the joins between them are
   correct. The gold-layer validation here checks row-count parity against
   silver, orphaned foreign keys per dimension, and duplicate grain — the
   three failure modes that actually break a star schema in production.

## Honest assessment — what's production-grade vs. what's a portfolio shortcut

Being direct about this matters more than it looks like it does. A reviewer
who can tell you exactly where their own pipeline would fall over is a
stronger signal than a pipeline that has no visible seams.

**Holds up in production as-is:**
- The content-hash key strategy
- The config-driven, single-conformed-table silver design
- The reject-table pattern for validation failures
- `DECIMAL` for money instead of float

**Works here, would need to change before this pipeline ran incrementally
instead of full-rebuild:**
- `dim_card` / `dim_location` / `dim_status` assign surrogate keys with
  `ROW_NUMBER()` over a full `CREATE OR REPLACE` every run. That's fine when
  every run rebuilds the world. It breaks the instant this becomes an
  incremental/streaming load — a newly-seen city can renumber every existing
  `location_id`, silently invalidating every fact row that already pointed at
  the old numbers. The fix is a durable key (hash of the natural key) or a
  `MERGE`-based key assignment that only ever appends new keys.
- `MAX_PLAUSIBLE_AMOUNT` is a bare literal (`5_000_000`), not derived from an
  actual distribution of the data. It works today because nobody's audited
  whether it's the right number. That's the kind of thing that looks done in
  a code review and isn't.

## Known issues

- **`fees` is likely always NULL for `trx_extra_10k`.** Bronze source 3's raw
  schema names the column `field`; silver's conforming step looks for a
  column named `fees`, doesn't find it, and defaults it to NULL for every
  row — including the one source meant to carry real fee data. This needs a
  source-side decision (was `field` supposed to be `fees`?) before any
  downstream analysis touches fees. Caught by inspection, not by the
  pipeline's own validation — which is itself worth noting: `flag_rejects()`
  checks for missing/invalid values, but a column that's *structurally never
  populated* due to a name mismatch produces no reject, no error, no
  warning. It just quietly reports zero everywhere. That's a gap in the
  validation coverage, not just a data bug.

## Tech stack

Databricks · PySpark (DataFrame API + Spark SQL) · Delta Lake · Unity Catalog
· dimensional modeling (star schema)

## Repo structure

```
1_dim_bronze_it.ipynb      # bronze: raw ingestion, per-source schema, 4 sources
1_dim_silver_it_v2.ipynb   # silver: config-driven clean/validate/conform/key
1_dim_gold_it_v2.ipynb     # gold: star schema build + join validation
DECISIONS.md               # running log of engineering decisions and trade-offs
```

## How to run

1. Land the four source CSVs under the paths referenced in
   `1_dim_bronze_it.ipynb` (`/Volumes/source_data/default/raw/iran_transactions/tr_1..4/`).
2. Run `1_dim_bronze_it.ipynb` → `bronze.brz_transactions_1..4`.
3. Run `1_dim_silver_it_v2.ipynb` → `silver.slv_transactions` +
   `silver.slv_transactions_rejected`.
4. Run `1_dim_gold_it_v2.ipynb` → dimension tables + `gold.fact_transactions`,
   then runs its own post-build validation and prints the results.

Assumes a Unity Catalog `iranian_transactions` with `bronze`/`silver`/`gold`
schemas already created.

## Author

Joseph Uzoma — statistics graduate (University of Ibadan), freelance creative/AI
practitioner building out a data engineering portfolio.
