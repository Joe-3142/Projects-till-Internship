import pyspark.sql.functions as F
from pyspark.sql.window import Window

catalog_name = "iranian_transactions"


def upsert_dimension(spark, table_name, natural_key_col, surrogate_key_col, distinct_values_df):
    """Stable surrogate-key assignment for a reference dimension.

    First run: bootstrap the table, numbering the natural key values 1..N
    in sorted order this is the same result ROW_NUMBER() would have given me.

    Every run after that: existing natural-key values are left completely
    untouched (they keep whatever surrogate key they were already given).
    Only natural-key values that don't already exist in the table get a new
    surrogate key, continuing from the current max. Nothing already written
    to fact_transactions (or any other table that joins on this key) is
    ever invalidated by a later run of this function.

    distinct_values_df must have exactly one column, `natural_key_col`.
    """
    full_table = f"{catalog_name}.gold.{table_name}"

    if not spark.catalog.tableExists(full_table):
        (
            distinct_values_df
            .withColumn(surrogate_key_col, F.row_number().over(Window.orderBy(natural_key_col)))
            .select(surrogate_key_col, natural_key_col)
            .write.format("delta").mode("overwrite").saveAsTable(full_table)
        )
        print(f"  {table_name}: bootstrapped with {distinct_values_df.count()} rows")
        return

    existing = spark.table(full_table)

    # left_anti keeps only rows from distinct_values_df with NO match in
    # existing -- i.e. genuinely new natural-key values.
    new_values = distinct_values_df.join(existing.select(natural_key_col), on=natural_key_col, how="left_anti")
    new_count = new_values.count()

    if new_count == 0:
        print(f"  {table_name}: no new values, {existing.count()} rows unchanged")
        return

    max_key = existing.agg(F.max(surrogate_key_col)).collect()[0][0] or 0
    new_rows = (
        new_values
        .withColumn(surrogate_key_col, F.row_number().over(Window.orderBy(natural_key_col)) + F.lit(max_key))
        .select(surrogate_key_col, natural_key_col)
    )

    # Append only -- existing rows are never rewritten, which is the whole
    # point. This is the dimension-table equivalent of an INSERT-only MERGE
    # (WHEN NOT MATCHED THEN INSERT, no WHEN MATCHED clause at all).
    new_rows.write.format("delta").mode("append").saveAsTable(full_table)
    print(f"  {table_name}: added {new_count} new rows, {existing.count() + new_count} total")


def build_dim_date(spark):
    spark.sql(f"""
        CREATE OR REPLACE TABLE {catalog_name}.gold.dim_date
        USING DELTA AS
        WITH dates AS (
          SELECT explode(sequence(to_date('2024-01-01'), to_date('2025-12-31'), interval 1 day)) AS full_date
        )
        SELECT
          CAST(date_format(full_date, 'yyyyMMdd') AS INT) AS date_sk,
          full_date,
          year(full_date) AS year,
          quarter(full_date) AS quarter,
          month(full_date) AS month,
          day(full_date) AS day,
          date_format(full_date, 'EEEE') AS day_name,
          date_format(full_date, 'MMMM') AS month_name,
          weekofyear(full_date) AS week_of_year,
          CASE WHEN dayofweek(full_date) IN (1, 7) THEN true ELSE false END AS is_weekend
        FROM dates
    """)
    print("dim_date built")


def build_dim_time(spark):
    spark.sql(f"""
        CREATE OR REPLACE TABLE {catalog_name}.gold.dim_time
        USING DELTA AS
        SELECT
          (hour * 100 + minute) AS time_id,
          hour,
          minute,
          CASE
            WHEN hour BETWEEN 5 AND 11  THEN 'Morning'
            WHEN hour BETWEEN 12 AND 16 THEN 'Afternoon'
            WHEN hour BETWEEN 17 AND 20 THEN 'Evening'
            ELSE 'Night'
          END AS time_of_day
        FROM (SELECT explode(sequence(0, 23)) AS hour) h
        CROSS JOIN (SELECT explode(sequence(0, 59)) AS minute) m
    """)
    print("dim_time built")


def build_dim_card(spark):
    distinct_cards = (
        spark.table(f"{catalog_name}.silver.slv_transactions")
        .filter(F.col("card_type").isNotNull() & (F.trim(F.col("card_type")) != ""))
        .select(F.trim(F.col("card_type")).alias("card_provider"))
        .distinct()
    )
    upsert_dimension(spark, "dim_card", "card_provider", "card_id", distinct_cards)


def build_dim_location(spark):
    distinct_cities = (
        spark.table(f"{catalog_name}.silver.slv_transactions")
        .filter(F.col("city").isNotNull() & (F.trim(F.col("city")) != ""))
        .select(F.trim(F.col("city")).alias("city_name"))
        .distinct()
    )
    upsert_dimension(spark, "dim_location", "city_name", "location_id", distinct_cities)


def build_dim_status(spark):
    distinct_statuses = (
        spark.table(f"{catalog_name}.silver.slv_transactions")
        .filter(F.col("status").isNotNull() & (F.trim(F.col("status")) != ""))
        .select(F.trim(F.col("status")).alias("status_label"))
        .distinct()
    )
    upsert_dimension(spark, "dim_status", "status_label", "status_id", distinct_statuses)


def build_fact_transactions(spark):
    spark.sql(f"""
        CREATE OR REPLACE TABLE {catalog_name}.gold.fact_transactions
        USING DELTA AS
        SELECT
          t.transaction_key,               -- grain key: content hash from silver, not raw id
          t.source_system,
          t.id                    AS source_id,   -- lineage/audit only -- NOT a key, see silver notes

          d.date_sk,
          tm.time_id,
          c.card_id,
          l.location_id,
          s.status_id,

          t.amount,
          COALESCE(t.fees, 0)            AS fees,
          COALESCE(t.discount, 0)        AS discount,
          COALESCE(t.balance_before, 0)  AS balance_before,
          t.is_debt,
          d.is_weekend,                   -- from dim_date's calendar logic, not the source column
                                           -- (only trx_extra_10k ever populated is_weekend directly)

          t._source_file,
          t.ingested_at
        FROM {catalog_name}.silver.slv_transactions t
        LEFT JOIN {catalog_name}.gold.dim_date     d  ON t.transaction_date = d.full_date
        LEFT JOIN {catalog_name}.gold.dim_time     tm ON t.hour = tm.hour AND t.minute = tm.minute
        LEFT JOIN {catalog_name}.gold.dim_card     c  ON t.card_type = c.card_provider
        LEFT JOIN {catalog_name}.gold.dim_location l  ON t.city = l.city_name
        LEFT JOIN {catalog_name}.gold.dim_status   s  ON t.status = s.status_label
    """)
    print("fact_transactions built")


def validate(spark):
    """Checks that actually exercise the join, not just a count() per table
    with no relationship between the numbers."""
    silver = spark.table(f"{catalog_name}.silver.slv_transactions")
    fact = spark.table(f"{catalog_name}.gold.fact_transactions")

    silver_count = silver.count()
    fact_count = fact.count()
    print(f"\nsilver rows: {silver_count}   fact rows: {fact_count}")
    if silver_count != fact_count:
        raise ValueError(
            "Row count mismatch between silver and fact -- a join is fanning "
            "out (a dimension has duplicate natural-key values) or dropping "
            "rows (an INNER JOIN got used somewhere instead of LEFT JOIN)."
        )

    orphan_checks = {
        "date_sk": "dim_date",
        "time_id": "dim_time",
        "card_id": "dim_card",
        "location_id": "dim_location",
        "status_id": "dim_status",
    }
    print("\nOrphan check (fact rows with no matching dimension row):")
    any_orphans = False
    for fk_col, dim_name in orphan_checks.items():
        orphan_count = fact.filter(F.col(fk_col).isNull()).count()
        flag = "  <-- investigate" if orphan_count else ""
        print(f"  {fk_col:15s} -> {dim_name:14s} orphans: {orphan_count:>6}{flag}")
        any_orphans = any_orphans or orphan_count > 0
    if any_orphans:
        print(
            "  Orphans usually mean a value in silver doesn't match anything "
            "in the dimension -- e.g. a city/card_type/status spelling that "
            "slipped past the silver cleaning maps. Check CITY_MAP / "
            "CARD_TYPE_MAP / STATUS_MAP in silver_transform.py first."
        )

    dupe_count = (
        fact.groupBy("transaction_key").count().filter(F.col("count") > 1).count()
    )
    if dupe_count > 0:
        raise ValueError(
            f"{dupe_count} duplicate transaction_key values in fact_transactions "
            "-- the fact table should be at the same grain as silver."
        )

    print("\nDimension row counts:")
    for name in ["dim_date", "dim_time", "dim_card", "dim_location", "dim_status"]:
        print(f"  {name:14s} {spark.table(f'{catalog_name}.gold.{name}').count()}")


def main():
    build_dim_date(spark)
    build_dim_time(spark)
    build_dim_card(spark)
    build_dim_location(spark)
    build_dim_status(spark)
    build_fact_transactions(spark)
    validate(spark)
    print("\nGold layer rebuilt against unified silver.slv_transactions.")


if __name__ == "__main__":
    main()
