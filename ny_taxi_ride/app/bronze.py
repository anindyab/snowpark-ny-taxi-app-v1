from __future__ import annotations

from snowflake.snowpark import Session
from snowflake.snowpark.functions import (
    col, lit, current_timestamp, call_function, hash
)


def bronze_ingest_procedure(session: Session) -> str:
    """
    Ingests parquet files from the configured stage into the bronze layer.
    - Reads data from @AWS_ETL_PARQUET_STAGE.
    - Adds ride_id and load_time columns.
    - Filters valid records and writes rejected records to BRONZE_NY_TAXI_RIDES_REJECTS.
    - Logs the load operation in BRONZE_LOAD_LOG.

    Returns:
        str: Status message indicating completion.
    """

    parquet_stage = "@AWS_ETL_PARQUET_STAGE"

    # Read from stage
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
        raise ValueError(
            f"Stage {parquet_stage} does not exist. Please check your configuration."
        ) from e

    # Add ride_id and load_time
    df = df.with_column(
        "ride_id",
        hash(
            col("pickup_datetime"),
            col("dropoff_datetime"),
            col("vendor_id"),
            col("trip_distance"),
            col("file_name")
        ).cast("STRING")
    ).with_column("load_time", current_timestamp())

    # Extract ride_month and ride_type using REGEXP_SUBSTR
    df = df.with_column(
        "ride_month", call_function("REGEXP_SUBSTR", col("file_name"), r"\d{4}-\d{2}")
    ).with_column(
        "ride_type", call_function("REGEXP_SUBSTR", col("file_name"), r"^(yellow|green|fhv)")
    )

    # Reject invalid rows
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

    # Save rejects
    reject_df.write.mode("append").save_as_table("BRONZE_NY_TAXI_RIDES_REJECTS")

    # Valid rows
    valid_df = df.filter(
        (col("pickup_datetime").is_not_null()) &
        (col("dropoff_datetime").is_not_null()) &
        (col("fare_amount") > 0)
    ).drop_duplicates(["ride_id"])

    # Log counts
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