# v1 → v2: What Was Wrong, What Changed

This documents the concrete, code-level problems in the original (v1)
silver and gold notebooks, and what replaced them in v2. Bronze is not
covered below because it's unchanged between versions — the same four
`StructType`-per-source ingestion cells exist in both.

## TL;DR

| | v1 | v2 |
|---|---|---|
| Sources actually reaching silver | 1 of 4 (`trx_10k` only) | 4 of 4 |
| Sources actually reaching gold | 0 (no gold tables existed) | 4 of 4, via unified silver |
| Row-level validation | none | every row validated, rejects routed with a reason |
| Grain key | raw `id` (reused ~100× per value in `trx_10k`) | content-hash `transaction_key` |
| Cleaning method | order-dependent `regexp_replace` chains | central case-insensitive lookup maps |
| `is_debt` / `Debt` | duplicate column storing `abs(amount)` | single derived boolean, no duplication |
| Gold tables | none — ad-hoc `display()` queries only | 5 dimensions + 1 fact, all persisted Delta tables |
| Post-build checks | none | row-count parity, orphan checks, dedup check |

---

## Silver

### 1. Three of four sources never reached silver

v1's silver notebook reads exactly one bronze table:

```python
df_bronze = spark.table(f'{catalog_name}.bronze.brz_transactions_1')
```

`brz_transactions_2`, `_3`, and `_4` are never referenced anywhere in the
notebook. `silver.slv_transactions` in v1 is really just a cleaned copy
of `trx_10k` — roughly a quarter of the raw data — with no indication
anywhere downstream that the other three sources were silently dropped.

**v2:** a config-driven loop (`SOURCE_CONFIGS`) transforms all four bronze
tables through the same function and unions them by name into one table.
Adding a fifth source is a dict entry, not a new set of hand-copied cells.

### 2. Cleaning was a fragile, order-dependent regex chain

v1's `card_type` cleaning:

```python
df_silver = df_silver.withColumn("card_type", F.initcap(F.col("card_type")))\
    .withColumn("card_type", F.regexp_replace(F.col("card_type"), "Mastercard", "Master Card"))\
    .withColumn("card_type", F.regexp_replace(F.col("card_type"), "Mastcard", "Master Card"))\
    .withColumn("card_type", F.regexp_replace(F.col("card_type"), "Master-card", "Master Card"))\
    .withColumn("card_type", F.regexp_replace(F.col("card_type"), "Vsa", "Visa Card"))\
    .withColumn("card_type", F.regexp_replace(F.col("card_type"), "Visa", "Visa Card"))\
    .withColumn("card_type", F.regexp_replace(F.col("card_type"), "Visa Card Card", "Visa Card"))
```

The last line exists only to undo damage the line before it caused
(`"Visa"` gets matched and rewritten again inside strings that were
already fixed to `"Visa Card"`, producing `"Visa Card Card"`). This is a
chain of substring replacements, not exact-value matches — every new
misspelling means reasoning about where in the chain to insert a new
line without it colliding with an earlier one. `city` cleaning has the
same shape (`regexp_replace` on substrings like `"Thr"` → `"Tehran"`),
which risks matching inside any city name that happens to contain that
substring.

A literal card_type value of `"Nan"` is visible in the v1 output and is
never cleaned or nulled out — it passes straight through as the string
`"Nan"`.

**v2:** `STATUS_MAP` / `CARD_TYPE_MAP` / `CITY_MAP` are flat dictionaries,
looked up case-insensitively on the *whole trimmed value*, with
`initcap(trim(...))` as a fallback for anything unmapped. Order doesn't
matter, and a new misspelling is one new key, not a new line to slot into
a chain.

### 3. A named rule was never actually implemented

v1 has this comment sitting above code that doesn't do what it says:

```python
#In the status column for transactions_1, we need to change the value of '0' to 'failed'
```

No such replacement exists anywhere in the notebook — `status = '0'` (if
present) passes straight through as `"0"`.

**v2:** the reject-reason validation would catch this kind of leftover
junk value under `invalid_time`/`missing_amount`/`amount_out_of_range`/
`missing_id` where applicable, and `STATUS_MAP` is the single place such
a mapping would actually live.

### 4. `is_debt` was never derived; amount stayed a float

v1's amount-casting is present but commented out:

```python
#change the amount column to float type
#df_silver = df_silver.withColumn("amount", F.col("amount").cast(FloatType()))
```

The comment above the negative-amount check says values will be "tagged
as debt later on" — no such tag is ever created in silver. The debt flag
only shows up later, in gold, as a duplicate column (see below).

Extreme sentinel-looking values (`-999999`, `-5000`, `-1`) are displayed
in v1's output but never flagged, capped, or rejected — there is no
validation of any kind on `amount`, or on any other column.

**v2:** `amount` is cast to `Decimal(18,2)` (not float), `is_debt` is
derived once in silver as `amount < 0`, and every row is validated with
an explicit reject reason instead of passing through unconditionally.

### 5. No grain key, no dedup, no reject table

v1 keeps the raw `id` column as-is and writes every row straight to
`silver.slv_transactions.` There is no uniqueness check anywhere, and no
rejected-rows table — a row with a missing amount or an unparseable time
is silently included on equal footing with clean rows.

This matters more than it looks like it should: `trx_10k` has 10,000 rows
but only 99 distinct `id` values (each reused ~100×), so `id` was never a
usable identifier in the first place — v1 just never checked.

**v2:** `transaction_key` (a SHA-256 hash of the row's full business
content) is the real grain key, with a hard uniqueness gate before
writing. Every row is validated, and anything that fails goes to
`slv_transactions_rejected` with a `reject_reason` instead of disappearing
into either table unexplained.

---

## Gold

### 6. There is no gold layer in v1

v1's "gold" notebook creates zero tables. Every cell is a `display()` call
run directly against `silver.slv_transactions`:

```python
df_gold_transactions_success = df_gold_transactions.filter(F.col("status") == "Success")
display(df_gold_transactions_success)
```

Nothing here is a dimension, a fact table, or anything persisted — it's
exploratory analysis living where a modeled star schema was supposed to
be. Since v1's silver only ever contained `trx_10k`, every number in
these v1 "gold" queries — the per-city totals, per-card-type debt
averages, everything — reflects roughly a quarter of the real dataset,
with nothing in the notebook signaling that.

**v2:** `gold.dim_date`, `dim_time`, `dim_card`, `dim_location`,
`dim_status`, and `gold.fact_transactions` are real, persisted Delta
tables, rebuilt via `CREATE OR REPLACE` and joined on natural keys.

### 7. `Debt` duplicates the sign already in `amount`

```python
df_gold_transactions = df_gold_transactions.withColumn(
    "Debt", F.when(F.col("amount") < 0, F.col("amount")*-1).otherwise(0)
)
```

This is a second column carrying information `amount`'s sign already
gives you, with its own opportunity to drift out of sync with `amount`
after any future edit.

**v2:** `is_debt` is a single boolean, derived once, in silver — not
duplicated in gold as a second numeric column.

### 8. Dead code and comment/code mismatches

- `df_gold_transactions.groupBy("city").sum("amount")...display` — no
  parentheses on `.display`, so this line silently does nothing.
- Comments like *"Find the total number of people that are in debt"*
  sit above `groupBy("city").sum("Debt")` — that's a sum over
  transactions, not a count of distinct people; there's no person-level
  identifier in the data at all, so the comment describes an analysis
  the code doesn't actually perform.
- Several blocks (per-city sum, per-card-type sum) are copy-pasted with
  minor typos (`"fir"` instead of `"for"`), suggesting they were adapted
  from each other under time pressure rather than written as reusable
  logic.

**v2:** the equivalent aggregations are just `GROUP BY` queries against
`gold.fact_transactions` joined to the relevant dimension — reusable,
correct by construction, and not living inside the pipeline notebook
itself.

### 9. No validation of the join at all

v1 never joins anything (there's nothing to join into), so there's no
equivalent of an orphan check, a row-count parity check, or a duplicate
check.

**v2:** every gold build ends with row-count parity against silver, an
orphan check per foreign key, and a duplicate check on `transaction_key`
at the fact grain — see [DECISIONS.md](DECISIONS.md#11-post-build-validation-checks-real-things).

---

## What this means for anyone who used v1's numbers

Any total, average, or breakdown produced by v1's gold notebook was
computed over `trx_10k` alone — about 9,700–10,000 of the ~19,800 rows
that exist across all four sources, with the other three completely
absent and no signal in the notebook that they were missing. Numbers from
v1 and v2 are not directly comparable for that reason alone, before even
accounting for the cleaning and validation differences above.
