from snowflake.snowpark import Session
from snowflake.snowpark.functions import (
    col, lit
)
from snowflake.snowpark import WhenMatchedClause, WhenNotMatchedClause

def dim_location_ingest(session: Session) -> str:
    """
    Ingests pickup and dropoff location data from LOCATION_REFERENCE and SILVER_NY_TAXI_RIDES,
    and populates unified LOCATION_DIM with role-based tagging.
    Returns:
        str: Status message indicating completion.
    """

    try:
        silver_df = session.table("SILVER_NY_TAXI_RIDES")
        ref_df = session.table("LOCATION_REFERENCE")
    except Exception as e:
        return f"Error reading source tables: {e}"

    # Extract distinct pickup and dropoff location IDs
    pickup_ids = silver_df.select(col("pickup_location_id").alias("location_id")).distinct()
    dropoff_ids = silver_df.select(col("dropoff_location_id").alias("location_id")).distinct()

    # Tag each with location_type
    pickup_dim = pickup_ids.with_column("location_type", lit("pickup"))
    dropoff_dim = dropoff_ids.with_column("location_type", lit("dropoff"))

    # Union both roles
    location_ids = pickup_dim.union(dropoff_dim).distinct()

    # Join with reference data
    enriched_locations = location_ids.join(ref_df, location_ids["location_id"] == ref_df["LOCATION_ID"]) \
        .select(
            location_ids["location_id"].cast("int").alias("LOCATION_ID"),
            location_ids["location_type"].alias("LOCATION_TYPE"),
            ref_df["BOROUGH"],
            ref_df["ZONE"],
            ref_df["SERVICE_ZONE"],
            ref_df["LATITUDE"],
            ref_df["LONGITUDE"]
        )

    # Merge into LOCATION_DIM
    dim_table = session.table("LOCATION_DIM")
    dim_table.merge(
        source=enriched_locations,
        join_expr=(dim_table["LOCATION_ID"] == enriched_locations["LOCATION_ID"]) &
                   (dim_table["LOCATION_TYPE"] == enriched_locations["LOCATION_TYPE"]),
        when_matched=[
            WhenMatchedClause(update={
                "BOROUGH": enriched_locations["BOROUGH"],
                "ZONE": enriched_locations["ZONE"],
                "SERVICE_ZONE": enriched_locations["SERVICE_ZONE"],
                "LATITUDE": enriched_locations["LATITUDE"],
                "LONGITUDE": enriched_locations["LONGITUDE"]
            })
        ],
        when_not_matched=[
            WhenNotMatchedClause(insert={
                "LOCATION_ID": enriched_locations["LOCATION_ID"],
                "LOCATION_TYPE": enriched_locations["LOCATION_TYPE"],
                "BOROUGH": enriched_locations["BOROUGH"],
                "ZONE": enriched_locations["ZONE"],
                "SERVICE_ZONE": enriched_locations["SERVICE_ZONE"],
                "LATITUDE": enriched_locations["LATITUDE"],
                "LONGITUDE": enriched_locations["LONGITUDE"]
            })
        ]
    )

    # Log the load
    row_count = enriched_locations.count()
    session.sql("""
        INSERT INTO DIMENSION_LOAD_LOG (
            dimension_name, load_start_time, load_end_time, row_count, status, error_message
        )
        VALUES (?, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), ?, ?, ?)
    """).bind(["LOCATION_DIM", row_count, "success", None]).collect()

    return "Location Dimension table successfully merged/updated."