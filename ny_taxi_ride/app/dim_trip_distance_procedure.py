from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, lit, when, when_matched, when_not_matched
from datetime import datetime

def dim_trip_distance_ingest(session: Session) -> str:
    """
    Ingests distinct trip distances from the silver layer and populates TRIP_DISTANCE_DIM.
    Adds distance buckets and merges into the dimension table.
    Returns:
        str: Status message indicating completion.
    """
    start_timestamp = datetime.now()

    try:
        silver_df = session.table("SILVER_NY_TAXI_RIDES")
    except Exception as e:
        return f"Error reading from silver layer: {e}"

    # Extract distinct trip distances, coalescing nulls to -1
    distance_df = silver_df.select(
        when(col("TRIP_DISTANCE").is_null(), lit(-1)).otherwise(col("TRIP_DISTANCE")).alias("TRIP_DISTANCE")
    ).distinct()

    # Add distance bucket
    distance_df = distance_df.with_column(
        "DISTANCE_BUCKET",
        when(col("TRIP_DISTANCE") == -1, lit("unknown"))
        .when(col("TRIP_DISTANCE") < 1, lit("short"))
        .when(col("TRIP_DISTANCE").between(1, 5), lit("medium"))
        .when(col("TRIP_DISTANCE").between(5, 15), lit("long"))
        .when(col("TRIP_DISTANCE") > 15, lit("very_long"))
        .otherwise(lit("unclassified"))
    )

    # Rename columns to match TRIP_DISTANCE_DIM
    distance_df = distance_df.select(
        col("TRIP_DISTANCE").cast("float").alias("TRIP_DISTANCE"),
        col("DISTANCE_BUCKET").alias("DISTANCE_BUCKET")
    )

    # Merge into TRIP_DISTANCE_DIM
    dim_table = session.table("TRIP_DISTANCE_DIM")
    dim_table.merge(
        source=distance_df,
        join_expr=dim_table["TRIP_DISTANCE"] == distance_df["TRIP_DISTANCE"],
        clauses=[
            when_matched().update({"DISTANCE_BUCKET": distance_df["DISTANCE_BUCKET"]}),
            when_not_matched().insert({
                "TRIP_DISTANCE": distance_df["TRIP_DISTANCE"],
                "DISTANCE_BUCKET": distance_df["DISTANCE_BUCKET"]
            })
        ]
    )

    # Log the load  
    end_timestamp = datetime.now()
    dimension_name = "TRIP_DISTANCE_DIM"
    row_count = distance_df.count()
    
    session.sql(f"""
        INSERT INTO DIMENSION_LOAD_LOG (
            dimension_name, load_start_time, load_end_time, row_count, status, error_message
        )
        VALUES (
            '{dimension_name}','{start_timestamp}', '{end_timestamp}', {row_count}, 'success', NULL
        )
    """).collect()

    return "Trip Distance Dimension table successfully merged/updated."