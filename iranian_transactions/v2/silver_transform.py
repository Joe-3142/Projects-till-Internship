import pyspark.sql.functions as F
from pyspark.sql.types import DecimalType, IntegerType

catalog_name = "iranian_transactions"

# ---------------------------------------------------------------------------
# 1. Central cleaning maps
# ---------------------------------------------------------------------------

STATUS_MAP = {
    "failed": "Fail", "fail": "Fail",
    "succeed": "Success", "success": "Success",
    "pending": "Pending", "processing": "Processing",
}

CARD_TYPE_MAP = {
    "mastercard": "Master Card", "mastcard": "Master Card",
    "master-card": "Master Card", "master card": "Master Card",
    "vsa": "Visa Card", "visa": "Visa Card", "visa card": "Visa Card",
    "discover": "Discover", "nano": "Nano",
}

CITY_MAP = {
    "thr": "Tehran", "tehr@n": "Tehran", "thran": "Tehran",
    "tehranan": "Tehran", "tehran": "Tehran",
    # Add real variants as you find them -- don't hardcode a closed
    # known_cities allowlist like the old cell 7 did; that broke on every
    # city that wasn't Tehran/Isfahan/Tabriz even though the raw data has
    # Karaj, Sanandaj, and others. A mapping table degrades gracefully:
    # unmapped values pass through untouched instead of getting rejected.
}

MAX_PLAUSIBLE_AMOUNT = 50_000_000  # tune against a real percentile check,
                                    # this is a placeholder ceiling


def _build_lookup_expr(mapping: dict):
    """This will build a case-insensitive F.create_map lookup for a cleaning dictionary."""
    pairs = []
    for k, v in mapping.items():
        pairs += [F.lit(k), F.lit(v)]
    return F.create_map(*pairs)


STATUS_LOOKUP = _build_lookup_expr(STATUS_MAP)
CARD_TYPE_LOOKUP = _build_lookup_expr(CARD_TYPE_MAP)
CITY_LOOKUP = _build_lookup_expr(CITY_MAP)


# ---------------------------------------------------------------------------
# 2. Reusable column-cleaning steps -- each one is a pure function I can
#    unit test in isolation.
# ---------------------------------------------------------------------------

def clean_status(df):
    key = F.lower(F.trim(F.col("status")))
    return df.withColumn(
        "status",
        F.coalesce(STATUS_LOOKUP[key], F.initcap(F.trim(F.col("status")))),
    )


def clean_card_type(df):
    key = F.lower(F.trim(F.col("card_type")))
    return df.withColumn(
        "card_type",
        F.coalesce(CARD_TYPE_LOOKUP[key], F.initcap(F.trim(F.col("card_type")))),
    )


def clean_city(df):
    key = F.lower(F.trim(F.col("city")))
    return df.withColumn(
        "city",
        F.coalesce(CITY_LOOKUP[key], F.initcap(F.trim(F.col("city")))),
    )


def parse_time(df, time_format: str):
    """First thing would be to parse the raw time string using a source-specific format, and derive
    the standard date/hour/minute/second breakdown columns. It uses
    try_to_timestamp so a bad value becomes NULL instead of blowing up the
    whole job - the null gets caught by the validation step below. I had so many issues with this too"""
    parsed = F.try_to_timestamp(F.col("time"), F.lit(time_format))
    return (
        df.withColumn("time_raw", F.col("time"))
        .withColumn("transaction_ts", parsed)
        .withColumn("transaction_date", F.to_date(parsed))
        .withColumn("hour", F.hour(parsed))
        .withColumn("minute", F.minute(parsed))
        .withColumn("second", F.second(parsed))
    )


def clean_amount(df):
    return df.withColumn(
        "amount", F.col("amount").cast(DecimalType(18, 2))
    )


def add_debt_flag(df):
    """This would be a single source of truth for the debt signal: a boolean flag. Don't
    also duplicate the negative amount into a second `Debt` column - the
    sign is already recoverable from `amount` any time I would have need of it."""
    return df.withColumn("is_debt", F.col("amount") < 0)


def add_lineage(df, source_system: str):
    return df.withColumn("source_system", F.lit(source_system)).withColumn(
        "ingested_at", F.current_timestamp()
    )


def add_transaction_key(df):
    """Deterministic, content-based surrogate key for the fact grain.

    We can't trust `id` as a key (see design note #2 above) so the key is a
    hash of the row's actual business content instead. This is deliberate,
    not a workaround: two rows that are identical on every business column
    *should* collapse into the same key, because that's what a genuine
    duplicate transaction is. An `id` collision between two rows with
    different time/city/amount is not a duplicate -- it's just a reused
    reference number -- and this key correctly leaves those as distinct
    rows."""
    key_cols = [
        "source_system", "id", "status", "card_type", "city",
        "transaction_date", "hour", "minute", "second", "amount",
    ]
    return df.withColumn(
        "transaction_key",
        F.sha2(F.concat_ws("||", *[F.col(c).cast("string") for c in key_cols]), 256),
    )


# ---------------------------------------------------------------------------
# 3. Validation -- applied identically to every source. Anything that fails
#    goes to slv_transactions_rejected with a reason instead of silently
#    corrupting the clean table (e.g. a NULL amount summed as 0).
# ---------------------------------------------------------------------------

def flag_rejects(df):
    return df.withColumn(
        "reject_reason",
        F.when(
            F.col("time_raw").isNotNull() & (F.col("time_raw") != "") & F.col("transaction_ts").isNull(),
            "invalid_time",
        )
        .when(F.col("amount").isNull(), "missing_amount")
        .when(F.abs(F.col("amount")) > F.lit(MAX_PLAUSIBLE_AMOUNT), "amount_out_of_range")
        .when(F.col("id").isNull(), "missing_id")
        .otherwise(F.lit(None)),
    )


# ---------------------------------------------------------------------------
# 4. Per-source config -- adding a 5th file means adding one dict here.
#    `id_col` lets you point at whatever the source calls it; everything
#    downstream refers to the conformed `id` name.
# ---------------------------------------------------------------------------

SOURCE_CONFIGS = [
    {
        "source_system": "trx_10k",
        "bronze_table": "bronze.brz_transactions_1",
        "time_format": "yyyy-MM-dd HH:mm:ss",
    },
    {
        "source_system": "trx_99",
        "bronze_table": "bronze.brz_transactions_2",
        "time_format": "HH:mm yyyy-MM-dd",
    },
    {
        "source_system": "trx_extra_10k",
        "bronze_table": "bronze.brz_transactions_3",
        "time_format": "yyyy-MM-dd HH:mm:ss",
    },
    {
        "source_system": "trx_small",
        "bronze_table": "bronze.brz_transactions_4",
        "time_format": "HH:mm yyyy-MM-dd",
    },
]

# The full conformed column list every source is normalized to. Columns a
# given source doesn't produce (fees/discount/balance_before/is_weekend are
# only real for trx_extra_10k) get filled with a typed NULL rather than
# being dropped -- so the union below is guaranteed to line up.
CONFORMED_COLUMNS = [
    "transaction_key", "source_system", "id", "status", "card_type", "city",
    "transaction_date", "hour", "minute", "second",
    "amount", "is_debt",
    "fees", "discount", "balance_before", "is_weekend",
    "_source_file", "ingested_at",
]


def conform_columns(df):
    """Guarantee every expected column exists (typed NULL if missing) and
    select in a fixed order, so unioning sources never depends on column
    position matching by accident."""
    out = df
    optional_defaults = {
        "fees": F.lit(None).cast(DecimalType(18, 2)),
        "discount": F.lit(None).cast(DecimalType(18, 2)),
        "balance_before": F.lit(None).cast(DecimalType(18, 2)),
        "is_weekend": F.lit(None).cast("boolean"),
    }
    for col_name, default_expr in optional_defaults.items():
        if col_name not in out.columns:
            out = out.withColumn(col_name, default_expr)
        else:
            out = out.withColumn(col_name, F.col(col_name).cast(DecimalType(18, 2))
                                  if col_name != "is_weekend" else F.col(col_name).cast("boolean"))
    return out.select(*CONFORMED_COLUMNS)


# ---------------------------------------------------------------------------
# 5. Orchestration -- this replaces every hand-written per-source cell.
# ---------------------------------------------------------------------------

def transform_source(spark, cfg: dict):
    """Bronze -> cleaned, validated, conformed silver rows for one source."""
    df = spark.table(f"{catalog_name}.{cfg['bronze_table']}")

    df = df.withColumn("id", F.col("id").cast(IntegerType()))
    df = clean_status(df)
    df = clean_card_type(df)
    df = clean_city(df)
    df = parse_time(df, cfg["time_format"])
    df = clean_amount(df)
    df = add_debt_flag(df)
    df = add_lineage(df, cfg["source_system"])
    df = add_transaction_key(df)
    df = flag_rejects(df)

    rejected = df.filter(F.col("reject_reason").isNotNull())
    clean = df.filter(F.col("reject_reason").isNull()).drop("reject_reason", "time_raw", "transaction_ts")

    return conform_columns(clean), rejected.drop("time_raw", "transaction_ts")


def build_unified_silver(spark):
    clean_frames, rejected_frames = [], []

    for cfg in SOURCE_CONFIGS:
        clean_df, rejected_df = transform_source(spark, cfg)
        clean_frames.append(clean_df)
        rejected_frames.append(rejected_df)
        print(f"  {cfg['source_system']:15s}  clean={clean_df.count():>6}  rejected={rejected_df.count():>4}")

    unified_clean = clean_frames[0]
    for f in clean_frames[1:]:
        unified_clean = unified_clean.unionByName(f)

    unified_rejected = rejected_frames[0]
    for f in rejected_frames[1:]:
        unified_rejected = unified_rejected.unionByName(f, allowMissingColumns=True)

    # Diagnostic only -- NOT a hard gate. Shows id cardinality per source so
    # a pattern like trx_10k's (10,000 rows, 99 distinct ids) is visible on
    # every run instead of surfacing as a confusing crash later.
    print("\nid cardinality per source (rows vs distinct id vs distinct transaction_key):")
    (
        unified_clean.groupBy("source_system")
        .agg(
            F.count("*").alias("rows"),
            F.countDistinct("id").alias("distinct_ids"),
            F.countDistinct("transaction_key").alias("distinct_keys"),
        )
        .show(truncate=False)
    )

    # The real uniqueness gate: transaction_key is a hash of full business
    # content, so a collision means two rows are genuinely the same event --
    # not just two rows that happen to share a reused id.
    dupe_count = (
        unified_clean.groupBy("transaction_key")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )
    if dupe_count > 0:
        raise ValueError(
            f"{dupe_count} duplicate transaction_key values found -- these "
            "rows are identical on every business column, not just sharing "
            "an id. Might need to look at it further."
        )

    return unified_clean, unified_rejected


def main():
    print("Building unified silver.slv_transactions ...")
    unified_clean, unified_rejected = build_unified_silver(spark)

    unified_clean.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(f"{catalog_name}.silver.slv_transactions")

    unified_rejected.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(f"{catalog_name}.silver.slv_transactions_rejected")

    total_clean = unified_clean.count()
    total_rejected = unified_rejected.count()
    print(f"\nDone. clean={total_clean}  rejected={total_rejected}")
    print(
        "Reject reasons:\n",
        unified_rejected.groupBy("reject_reason").count().toPandas().to_string(index=False),
    )


if __name__ == "__main__":
    main()
