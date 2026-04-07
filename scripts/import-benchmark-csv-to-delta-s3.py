#!/usr/bin/env python3
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructField,
    StructType,
    IntegerType,
    LongType,
    FloatType,
    StringType,
    DateType,
    TimestampType,
)
import sys
import os


BUCKET_COLUMN = "_pixels_bucket_id"
DEFAULT_CONFIG_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "etc",
    "pixels-spark.properties",
)
HASH_BUCKET_COUNT_KEY = "pixels.spark.delta.hash-bucket.count"


TABLE_SPECS = [
    (
        "customer",
        "customer.csv",
        StructType(
            [
                StructField("custID", IntegerType(), True),
                StructField("companyID", IntegerType(), True),
                StructField("gender", StringType(), True),
                StructField("name", StringType(), True),
                StructField("age", IntegerType(), True),
                StructField("phone", StringType(), True),
                StructField("province", StringType(), True),
                StructField("city", StringType(), True),
                StructField("loan_balance", FloatType(), True),
                StructField("saving_credit", IntegerType(), True),
                StructField("checking_credit", IntegerType(), True),
                StructField("loan_credit", IntegerType(), True),
                StructField("Isblocked", IntegerType(), True),
                StructField("created_date", DateType(), True),
                StructField("last_update_timestamp", TimestampType(), True),
                StructField("freshness_ts", TimestampType(), True),
            ]
        ),
        ["custID"],
    ),
    (
        "company",
        "company.csv",
        StructType(
            [
                StructField("companyID", IntegerType(), True),
                StructField("name", StringType(), True),
                StructField("category", StringType(), True),
                StructField("staff_size", IntegerType(), True),
                StructField("loan_balance", FloatType(), True),
                StructField("phone", StringType(), True),
                StructField("province", StringType(), True),
                StructField("city", StringType(), True),
                StructField("saving_credit", IntegerType(), True),
                StructField("checking_credit", IntegerType(), True),
                StructField("loan_credit", IntegerType(), True),
                StructField("Isblocked", IntegerType(), True),
                StructField("created_date", DateType(), True),
                StructField("last_update_timestamp", TimestampType(), True),
                StructField("freshness_ts", TimestampType(), True),
            ]
        ),
        ["companyID"],
    ),
    (
        "savingAccount",
        "savingAccount.csv",
        StructType(
            [
                StructField("accountID", IntegerType(), True),
                StructField("userID", IntegerType(), True),
                StructField("balance", FloatType(), True),
                StructField("Isblocked", IntegerType(), True),
                StructField("ts", TimestampType(), True),
                StructField("freshness_ts", TimestampType(), True),
            ]
        ),
        ["accountID"],
    ),
    (
        "checkingAccount",
        "checkingAccount.csv",
        StructType(
            [
                StructField("accountID", IntegerType(), True),
                StructField("userID", IntegerType(), True),
                StructField("balance", FloatType(), True),
                StructField("Isblocked", IntegerType(), True),
                StructField("ts", TimestampType(), True),
                StructField("freshness_ts", TimestampType(), True),
            ]
        ),
        ["accountID"],
    ),
    (
        "transfer",
        "transfer.csv",
        StructType(
            [
                StructField("id", LongType(), True),
                StructField("sourceID", IntegerType(), True),
                StructField("targetID", IntegerType(), True),
                StructField("amount", FloatType(), True),
                StructField("type", StringType(), True),
                StructField("ts", TimestampType(), True),
                StructField("freshness_ts", TimestampType(), True),
            ]
        ),
        ["id"],
    ),
    (
        "checking",
        "checking.csv",
        StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("sourceID", IntegerType(), True),
                StructField("targetID", IntegerType(), True),
                StructField("amount", FloatType(), True),
                StructField("type", StringType(), True),
                StructField("ts", TimestampType(), True),
                StructField("freshness_ts", TimestampType(), True),
            ]
        ),
        ["id"],
    ),
    (
        "loanapps",
        "loanApps.csv",
        StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("applicantID", IntegerType(), True),
                StructField("amount", FloatType(), True),
                StructField("duration", IntegerType(), True),
                StructField("status", StringType(), True),
                StructField("ts", TimestampType(), True),
                StructField("freshness_ts", TimestampType(), True),
            ]
        ),
        ["id"],
    ),
    (
        "loantrans",
        "loanTrans.csv",
        StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("applicantID", IntegerType(), True),
                StructField("appID", IntegerType(), True),
                StructField("amount", FloatType(), True),
                StructField("status", StringType(), True),
                StructField("ts", TimestampType(), True),
                StructField("duration", IntegerType(), True),
                StructField("contract_timestamp", TimestampType(), True),
                StructField("delinquency", IntegerType(), True),
                StructField("freshness_ts", TimestampType(), True),
            ]
        ),
        ["id"],
    ),
]


def load_properties(path: str) -> dict[str, str]:
    properties: dict[str, str] = {}
    if not os.path.exists(path):
        return properties

    with open(path, "r", encoding="utf-8") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            properties[key.strip()] = value.strip()
    return properties


def resolve_hash_bucket_count() -> int:
    if "PIXELS_IMPORT_HASH_BUCKET_COUNT" in os.environ:
        return int(os.environ["PIXELS_IMPORT_HASH_BUCKET_COUNT"])

    config_path = os.environ.get("PIXELS_SPARK_CONFIG", DEFAULT_CONFIG_PATH)
    properties = load_properties(config_path)
    return int(properties.get(HASH_BUCKET_COUNT_KEY, "0"))


def main() -> int:
    if len(sys.argv) not in (3, 4):
        print(
            "Usage: import-benchmark-csv-to-delta-s3.py <csv-root> <delta-root> [table1,table2,...]",
            file=sys.stderr,
        )
        return 2

    csv_root = sys.argv[1].rstrip("/")
    delta_root = sys.argv[2].rstrip("/")
    selected_tables = None
    count_rows = os.environ.get("PIXELS_IMPORT_COUNT_ROWS", "").lower() in {"1", "true", "yes"}
    hash_bucket_count = resolve_hash_bucket_count()
    if len(sys.argv) == 4:
        selected_tables = {name.strip() for name in sys.argv[3].split(",") if name.strip()}

    spark = (
        SparkSession.builder.appName("pixels-benchmark-delta-import-s3")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

    try:
        for table_name, file_name, schema, primary_keys in TABLE_SPECS:
            if selected_tables is not None and table_name not in selected_tables:
                continue
            csv_path = f"{csv_root}/{file_name}"
            nested_csv_path = f"{csv_root}/{table_name}/{file_name}"
            if not os.path.exists(csv_path) and os.path.exists(nested_csv_path):
                csv_path = nested_csv_path
            delta_path = f"{delta_root}/{table_name}"
            dataset = (
                spark.read.format("csv")
                .schema(schema)
                .option("header", "false")
                .option("mode", "FAILFAST")
                .option("nullValue", "")
                .load(csv_path)
            )

            if hash_bucket_count > 0:
                dataset = dataset.withColumn(
                    BUCKET_COLUMN,
                    F.pmod(F.hash(*[F.col(pk) for pk in primary_keys]), F.lit(hash_bucket_count)),
                )

            writer = dataset.write.format("delta").mode("overwrite")
            if hash_bucket_count > 0:
                writer = writer.partitionBy(BUCKET_COLUMN)
            writer.save(delta_path)

            message = (
                f"table={table_name} csv_path={csv_path} delta_path={delta_path} "
                f"hash_bucket_count={hash_bucket_count}"
            )
            if count_rows:
                message = f"{message} row_count={dataset.count()}"
            print(message, flush=True)
    finally:
        spark.stop()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
