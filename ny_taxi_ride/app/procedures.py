from __future__ import annotations

import sys

from common import print_hello
from common import copy_to_table
from config import configs
from schema import schemas
from snowflake.snowpark import Session
from snowflake.snowpark.functions import uuid, current_timestamp
from snowflake.snowpark.functions import col, lit, when, to_timestamp, month, dayofweek, hour
from snowflake.snowpark.functions import hash
from snowflake.snowpark.functions import regexp_substr

def hello_procedure(session: Session, name: str) -> str:
    return print_hello(name)


def test_procedure(session: Session) -> str:
    return "Test procedure"

def test_procedure_two(session: Session) -> str:
    return "Test procedure two"

def copy_to_table_proc(session: Session)-> str:
    copied_into_result, qid = copy_to_table(session,configs.employee_config,schemas.emp_stg_schema)
    
def bronze_ingest_procedure(session: Session) -> str:
    """
    Ingests parquet files from the configured stage into the bronze layer.
    - Reads data from @AWS_ETL_PARQUET_STAGE.
    - Adds ride_id and load_time columns.
    - Filters valid records and writes rejected records to BRONZE_NY_TAXI_RIDES_REJECTS.
    - Logs the load operation in BRONZE_LOAD_LOG.
    Returns:
        str: Status message indicating completion.
    Side Effects:
        - Writes to BRONZE_NY_TAXI_RIDES_REJECTS table.
        - Inserts a row into BRONZE_LOAD_LOG table.
    """
    parquet_stage = "@AWS_ETL_PARQUET_STAGE"
    try:
        df = (
            session.read.parquet(parquet_stage)
            .with_column_renamed("VendorID", "vendor_id")
            .with_column_renamed("RatecodeID", "rate_code_id")
            .with_column_renamed("tpep_pickup_datetime", "pickup_datetime")
            .with_column_renamed("tpep_dropoff_datetime", "dropoff_datetime")
            .with_column_renamed("PULocationID", "pickup_location_id")
            .with_column_renamed("DOLocationID", "dropoff_location_id")
            .select("*", col("METADATA$FILENAME").cast("STRING").alias("file_name"))
        )
    except Exception as e:
        raise ValueError(f"Stage {parquet_stage} does not exist. Please check your configuration.") from e

    df = df.with_column("ride_id", hash(
                col("pickup_datetime"),
                col("dropoff_datetime"),
                col("vendor_id"),
                col("trip_distance"),
                col("file_name")
            ).cast("STRING")
        ).with_column("load_time", current_timestamp())

    # Convert datetime columns to TIMESTAMP_NTZ
    df = df.with_column(
            "ride_month", regexp_substr(col("file_name"), r"\d{4}-\d{2}")
        ).with_column(
            "ride_type", regexp_substr(col("file_name"), r"^(yellow|green|fhv)")
        )

    
    # Before writing to the rejects table
    reject_df = (
        df.filter(
            (col("pickup_datetime").is_null()) |
            (col("dropoff_datetime").is_null()) |
            (col("fare_amount") <= 0)
        )
        .with_column("rejection_reason", lit("Missing datetime or invalid fare amount"))
        .with_column("rejection_time", current_timestamp())
        .with_column("rejection_stage", lit("bronze_ingest"))
    )

    # Now, write the enriched reject_df to your rejects table
    reject_df.write.mode("append").save_as_table("BRONZE_NY_TAXI_RIDES_REJECTS")
    
    # Parameterize values for SQL insert to avoid SQL injection
    valid_df = df.filter(
        (col("pickup_datetime").is_not_null()) &
        (col("dropoff_datetime").is_not_null()) &
        (col("fare_amount") > 0)
    )
    valid_df = valid_df.drop_duplicates(["ride_id"])

    row_count = valid_df.count()
    rejected_count = reject_df.count()

    session.sql(f"""
        INSERT INTO BRONZE_LOAD_LOG (
        load_start_time, load_end_time, row_count, rejected_row_count, status, error_message
        )
        VALUES (
        CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), {row_count}, {rejected_count}, 'success', NULL
        )
        """).collect()
    
    return "Bronze ingest complete"
    

def silver_clean_procedure(session: Session) -> str:
    df = session.table("BRONZE_NY_TAXI_RIDES")
    cleaned = (
        df.filter((col("pickup_datetime").is_not_null()) & (col("dropoff_datetime").is_not_null()))
          .filter(col("pickup_datetime") < col("dropoff_datetime"))
          .filter((col("trip_distance") > 0.1) & (col("fare_amount") > 0))
          .with_column("ride_duration_minutes", (col("dropoff_datetime") - col("pickup_datetime")) / 60)
          .with_column("avg_speed_mph", col("trip_distance") / (col("ride_duration_minutes") / 60))
    )
    cleaned.write.mode("overwrite").save_as_table("SILVER_NY_TAXI_RIDES")
    return "Silver transformation complete"

def gold_model_procedure(session: Session) -> str:
    df = session.table("SILVER_NY_TAXI_RIDES")
    fact_df = df.select(
        col("pickup_datetime"),
        col("dropoff_datetime"),
        col("fare_amount"),
        col("trip_distance"),
        col("ride_duration_minutes"),
        col("avg_speed_mph"),
        col("vendor_id"),
        col("payment_type")
    )
    fact_df.write.mode("overwrite").save_as_table("FACT_TAXI_RIDES")
    return "Gold layer built"


# For local debugging
# Beware you may need to type-convert arguments if you add input parameters
if __name__ == "__main__":
    # Create a local Snowpark session
    with Session.builder.config("local_testing", True).getOrCreate() as session:
        print(hello_procedure(session, *sys.argv[1:]))  # type: ignore
