from __future__ import annotations

from snowflake.snowpark import Session
from datetime import datetime
from snowflake.snowpark.functions import (
    col, lit, current_timestamp, call_function, hash, concat_ws, call_function
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

    # --- Define Stage and File Format ---
    parquet_stage = "@NYC_TAXI_RIDE_DB.TRIP_DATA.AWS_ETL_PARQUET_STAGE"
    start_timestamp = datetime.now()
    df = None       
    # Read from stage
    try:
        df = session.sql(f"""
            SELECT
                $1:"VendorID"::NUMBER AS VENDOR_ID,
                TO_TIMESTAMP_NTZ($1:"tpep_pickup_datetime"::BIGINT, 6) AS PICKUP_DATETIME,
                TO_TIMESTAMP_NTZ($1:"tpep_dropoff_datetime"::BIGINT, 6) AS DROPOFF_DATETIME,
                CAST($1:"passenger_count" AS INT) AS PASSENGER_COUNT,
                CAST($1:"trip_distance" AS FLOAT) AS TRIP_DISTANCE,
                CAST($1:"RatecodeID" AS INT) AS RATE_CODE_ID,
                CAST($1:"store_and_fwd_flag" AS STRING) AS STORE_AND_FWD_FLAG,
                CAST($1:"PULocationID" AS INT) AS PICKUP_LOCATION_ID,
                CAST($1:"DOLocationID" AS INT) AS DROPOFF_LOCATION_ID,
                CAST($1:"payment_type" AS INT) AS PAYMENT_TYPE,
                CAST($1:"fare_amount" AS FLOAT) AS FARE_AMOUNT,
                CAST($1:"extra" AS FLOAT) AS EXTRA,
                CAST($1:"mta_tax" AS FLOAT) AS MTA_TAX,
                CAST($1:"tip_amount" AS FLOAT) AS TIP_AMOUNT,
                CAST($1:"tolls_amount" AS FLOAT) AS TOLLS_AMOUNT,
                CAST($1:"improvement_surcharge" AS FLOAT) AS IMPROVEMENT_SURCHARGE,
                CAST($1:"total_amount" AS FLOAT) AS TOTAL_AMOUNT,
                CAST($1:"congestion_surcharge" AS FLOAT) AS CONGESTION_SURCHARGE,
                CAST($1:"Airport_fee" AS FLOAT) AS AIRPORT_FEE,
                CAST($1:"cbd_congestion_fee" AS FLOAT) AS CBD_CONGESTION_FEE,
                METADATA$FILENAME AS FILE_NAME
            FROM {parquet_stage}
            WHERE METADATA$FILENAME ILIKE '%yellow_tripdata_2025%'
        """)
        
        # Add ride_id and load_time
        df = df.with_column(
            "RIDE_ID",
            hash(
                concat_ws(
                    lit("_"),  # separator must be a literal, not a bare string
                    col("PICKUP_DATETIME"),
                    col("DROPOFF_DATETIME"),
                    col("VENDOR_ID"),
                    col("TRIP_DISTANCE"),
                    col("FILE_NAME")
                )
            ).cast("STRING")
        ).with_column("LOAD_TIME", current_timestamp())
        
        # Extract just the filename (strip directories)
        df = df.with_column(
            "BASE_NAME",
            call_function("REGEXP_SUBSTR", col("FILE_NAME"), r"[^/]+\.parquet$")
        )

        # Extract ride_month and ride_type using REGEXP_SUBSTR
        df = df.with_column(
            "RIDE_MONTH", call_function("REGEXP_SUBSTR", col("BASE_NAME"), r"\d{4}-\d{2}")
        ).with_column(
            "RIDE_TYPE", call_function("REGEXP_SUBSTR", col("BASE_NAME"), r"(yellow|green|fhv)")
        )
    except Exception as e:
        raise ValueError(
            f"Stage {parquet_stage} does not exist. Please check your configuration."
        ) from e

    # Reject invalid rows
    reject_df = (
        df.filter(
            (col("PICKUP_DATETIME").is_null()) |
            (col("DROPOFF_DATETIME").is_null()) |
            (col("FARE_AMOUNT") <= 0)
        )
        .with_column("REJECTION_REASON", lit("Missing datetime or invalid fare amount"))
        .with_column("REJECTION_TIME", current_timestamp())
        .with_column("REJECTION_STAGE", lit("bronze_ingest"))
    )

    # Save rejects
    reject_df.write.mode("append").save_as_table("BRONZE_NY_TAXI_RIDES_REJECTS")
    
    # Valid rows
    valid_df = df.filter(
        (col("PICKUP_DATETIME").is_not_null()) &
        (col("DROPOFF_DATETIME").is_not_null()) &
        (col("FARE_AMOUNT") > 0)
    ).drop_duplicates(["RIDE_ID"])
    
    # Save valid records
    valid_df.write.mode("append").save_as_table("BRONZE_NY_TAXI_RIDES")

    
     # Log counts
    row_count = valid_df.count()
    rejected_count = reject_df.count()
    end_timestamp = datetime.now()


    session.sql(f"""
        INSERT INTO BRONZE_LOAD_LOG (
            load_start_time, load_end_time, row_count, rejected_row_count, status, error_message
        )
        VALUES (
            '{start_timestamp}', '{end_timestamp}', {row_count}, {rejected_count}, 'success', NULL
        )
    """).collect()

    return "Bronze ingest complete"