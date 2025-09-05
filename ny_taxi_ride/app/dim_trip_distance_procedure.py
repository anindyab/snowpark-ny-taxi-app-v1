from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, lit, when
from snowflake.snowpark import WhenMatchedClause, WhenNotMatchedClause

def dim_trip_distance_ingest(session: Session) -> str:
    """
    Ingests distinct trip distances from the silver layer and populates TRIP_DISTANCE_DIM.
    Adds distance buckets and merges into the dimension table.
    Returns:
        str: Status message indicating completion.
    """

    try:
        silver_df = session.table("SILVER_NY_TAXI_RIDES")
    except Exception as e:
        return f"Error reading from silver layer: {e}"

    # Extract distinct trip distances, coalescing nulls to -1
    distance_df = silver_df.select(
        when(col("trip_distance").is_null(), lit(-1)).otherwise(col("trip_distance")).alias("trip_distance")
    ).distinct()

    # Add distance bucket
    distance_df = distance_df.with_column(
        "distance_bucket",
        when(col("trip_distance") == -1, lit("unknown"))
        .when(col("trip_distance") < 1, lit("short"))
        .when(col("trip_distance").between(1, 5), lit("medium"))
        .when(col("trip_distance").between(5, 15), lit("long"))
        .when(col("trip_distance") > 15, lit("very_long"))
        .otherwise(lit("unclassified"))
    )

    # Rename columns to match TRIP_DISTANCE_DIM
    distance_df = distance_df.select(
        col("trip_distance").cast("float").alias("TRIP_DISTANCE"),
        col("distance_bucket").alias("DISTANCE_BUCKET")
    )

    # Merge into TRIP_DISTANCE_DIM
    dim_table = session.table("TRIP_DISTANCE_DIM")
    dim_table.merge(
        source=distance_df,
        join_expr=dim_table["TRIP_DISTANCE"] == distance_df["TRIP_DISTANCE"],
        when_matched=[
            WhenMatchedClause(update={"DISTANCE_BUCKET": distance_df["DISTANCE_BUCKET"]})
        ],
        when_not_matched=[
            WhenNotMatchedClause(insert={
                "TRIP_DISTANCE": distance_df["TRIP_DISTANCE"],
                "DISTANCE_BUCKET": distance_df["DISTANCE_BUCKET"]
            })
        ]
    )

    # Log the load
    row_count = distance_df.count()
    session.sql("""
        INSERT INTO DIMENSION_LOAD_LOG (
            dimension_name, load_start_time, load_end_time, row_count, status, error_message
        )
        VALUES (?, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), ?, ?, ?)
    """).bind(["TRIP_DISTANCE_DIM", row_count, "success", None]).collect()

    return "Trip Distance Dimension table successfully merged/updated."