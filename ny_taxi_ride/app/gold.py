from __future__ import annotations

from snowflake.snowpark import Session
from fact_taxi_rides_procedure import fact_taxi_rides_ingest
from dim_datetime_procedure import dim_datetime_ingest
from dim_location_procedure import dim_location_ingest
from dim_passenger_count_procedure import dim_passenger_count_ingest
from dim_vendor_procedure import dim_vendor_ingest
from dim_trip_distance_procedure import dim_trip_distance_ingest
from dim_rate_code_procedure import dim_rate_code_ingest
import time

def gold_model_procedure(session: Session) -> str:
    """
    Orchestrates gold layer construction by sequentially loading dimensions and fact table.
    Adds logging to FACT_LOAD_LOG for full pipeline visibility.
    Returns:
        str: Status message indicating completion.
    """

    start_time = time.time()
    status = "success"
    error_message = None

    try:
        dim_rate_code_ingest(session)
        dim_datetime_ingest(session)
        dim_location_ingest(session)
        dim_passenger_count_ingest(session)
        dim_vendor_ingest(session)
        dim_trip_distance_ingest(session)
        fact_taxi_rides_ingest(session)
    except Exception as e:
        status = "failed"
        error_message = str(e)

    end_time = time.time()
    duration = round(end_time - start_time, 2)

    # Log the gold model build
    session.sql("""
        INSERT INTO FACT_LOAD_LOG (
            fact_table_name, load_start_time, load_end_time, row_count, status, error_message
        )
        VALUES (?, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), ?, ?, ?)
    """).bind([
        "FACT_TAXI_RIDES",
        0,  # row_count placeholder; could be updated if tracked
        status,
        error_message
    ]).collect()

    if status == "success":
        return f"Gold layer built successfully in {duration} seconds"
    else:
        return f"Gold layer build failed: {error_message}"

