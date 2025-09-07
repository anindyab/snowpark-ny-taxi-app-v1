from snowflake.snowpark import Session
from snowflake.snowpark.functions import (
    col, lit, when, when_matched, when_not_matched
)
from datetime import datetime

def dim_passenger_count_ingest(session: Session) -> str:
    """
    Ingests distinct passenger counts from the silver layer and populates PASSENGER_COUNT_DIM.
    Adds bucketing and merges into the dimension table.
    Returns:
        str: Status message indicating completion.
    """
    start_timestamp = datetime.now()

    try:
        silver_df = session.table("SILVER_NY_TAXI_RIDES")
    except Exception as e:
        return f"Error reading from silver layer: {e}"

    # Extract distinct passenger counts, coalescing nulls to -1
    passenger_df = silver_df.select(
        when(col("passenger_count").is_null(), lit(-1)).otherwise(col("passenger_count")).alias("passenger_count")
    ).distinct()

    # Add passenger bucket
    passenger_df = passenger_df.with_column(
        "passenger_bucket",
        when(col("passenger_count") == -1, lit("unknown"))
        .when(col("passenger_count") == 0, lit("zero"))
        .when(col("passenger_count") == 1, lit("solo"))
        .when(col("passenger_count").between(2, 3), lit("small_group"))
        .when(col("passenger_count").between(4, 6), lit("large_group"))
        .otherwise(lit("extra_large"))
    )

    # Rename columns to match dimension table
    passenger_df = passenger_df.select(
        col("passenger_count").cast("int").alias("PASSENGER_COUNT"),
        col("passenger_bucket").alias("PASSENGER_BUCKET")
    )

    # Merge into PASSENGER_COUNT_DIM
    dim_table = session.table("PASSENGER_COUNT_DIM")
    dim_table.merge(
        passenger_df,
        dim_table["PASSENGER_COUNT"] == passenger_df["PASSENGER_COUNT"],
        [
            when_matched().update({"PASSENGER_BUCKET": passenger_df["PASSENGER_BUCKET"]}),
            when_not_matched().insert({
                "PASSENGER_COUNT": passenger_df["PASSENGER_COUNT"],
                "PASSENGER_BUCKET": passenger_df["PASSENGER_BUCKET"]
            })
        ]
    )

    # Log the load
    end_timestamp = datetime.now()
    dimension_name = "PASSENGER_COUNT_DIM"
    row_count = passenger_df.count()
    
    session.sql(f"""
        INSERT INTO DIMENSION_LOAD_LOG (
             dimension_name, load_start_time, load_end_time, row_count, status, error_message
        )
        VALUES (
            '{dimension_name}','{start_timestamp}', '{end_timestamp}', {row_count}, 'success', NULL
        )
    """).collect()

    return "Passenger Count Dimension table successfully merged/updated."


