from snowflake.snowpark import Session
from datetime import datetime

from snowflake.snowpark.functions import (
    col, lit, when_matched, when_not_matched
)

def dim_location_ingest(session: Session) -> str:
    """
    Ingests pickup and dropoff location data from LOCATION_REFERENCE and SILVER_NY_TAXI_RIDES,
    and populates unified LOCATION_DIM with role-based tagging.
    Returns:
        str: Status message indicating completion.
    """
    start_timestamp = datetime.now()

    silver_df = session.table("SILVER_NY_TAXI_RIDES")
    ref_df = session.table("LOCATION_REFERENCE")

    # Extract distinct pickup and dropoff location IDs and tag each with a location_type
    pickup_dim = (
        silver_df.select(col("PICKUP_LOCATION_ID").alias("LOCATION_ID"))
        .distinct()
        .with_column("LOCATION_TYPE", lit("pickup"))
    )
    
    dropoff_dim = (
        silver_df.select(col("DROPOFF_LOCATION_ID").alias("LOCATION_ID"))
        .distinct()
        .with_column("LOCATION_TYPE", lit("dropoff"))
    )

    # Union both DataFrames, which now have the same schema
    location_ids = pickup_dim.union(dropoff_dim).distinct()
    
    # Join with reference data
    enriched_locations = location_ids.join(
        ref_df,
        location_ids["LOCATION_ID"] == ref_df["LOCATION_ID"],
        "left_outer"
    ).select(
        location_ids["LOCATION_ID"].alias("LOCATION_ID"),
        location_ids["LOCATION_TYPE"].alias("LOCATION_TYPE"),
        ref_df["BOROUGH"],
        ref_df["ZONE"],
        ref_df["SERVICE_ZONE"],
        ref_df["LATITUDE"],
        ref_df["LONGITUDE"]
    )
    
    # Merge into LOCATION_DIM
    dim_table = session.table("LOCATION_DIM")
    dim_table.merge(
        enriched_locations,
        (dim_table["LOCATION_ID"] == enriched_locations["LOCATION_ID"]) &
        (dim_table["LOCATION_TYPE"] == enriched_locations["LOCATION_TYPE"]),
        [
            when_matched().update({
                "BOROUGH": enriched_locations["BOROUGH"],
                "ZONE": enriched_locations["ZONE"],
                "SERVICE_ZONE": enriched_locations["SERVICE_ZONE"],
                "LATITUDE": enriched_locations["LATITUDE"],
                "LONGITUDE": enriched_locations["LONGITUDE"]
            }),
            when_not_matched().insert({
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

    end_timestamp = datetime.now()
    dimension_name = "LOCATION_DIM"
    # Log the load
    row_count = enriched_locations.count()   
    
    session.sql(f"""
        INSERT INTO DIMENSION_LOAD_LOG (
             dimension_name, load_start_time, load_end_time, row_count, status, error_message
        )
        VALUES (
            '{dimension_name}','{start_timestamp}', '{end_timestamp}', {row_count}, 'success', NULL
        )
    """).collect()

    return "Location Dimension table successfully merged/updated."