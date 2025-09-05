from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
from snowflake.snowpark import WhenNotMatchedClause

def fact_taxi_rides_ingest(session: Session) -> str:
    """
    Loads enriched taxi ride records from silver layer into FACT_TAXI_RIDES.
    Resolves surrogate keys from dimensions and inserts fact records.
    Returns:
        str: Status message indicating completion.
    """

    try:
        silver_df = session.table("SILVER_NY_TAXI_RIDES")
        vendor_dim = session.table("VENDOR_DIM")
        rate_code_dim = session.table("RATE_CODE_DIM")
        payment_type_dim = session.table("PAYMENT_TYPE_DIM")
        passenger_dim = session.table("PASSENGER_COUNT_DIM")
        distance_dim = session.table("TRIP_DISTANCE_DIM")
        datetime_dim = session.table("DATETIME_DIM")
        location_dim = session.table("LOCATION_DIM")
    except Exception as e:
        return f"Error reading source tables: {e}"

    # Resolve surrogate keys
    fact_df = silver_df \
        .join(vendor_dim, silver_df["vendor_id"] == vendor_dim["vendor"], "left") \
        .join(rate_code_dim, silver_df["rate_code_id"].cast("int") == rate_code_dim["RATECODEID"], "left") \
        .join(payment_type_dim, silver_df["payment_type"].cast("int") == payment_type_dim["PAYMENT_TYPE"], "left") \
        .join(passenger_dim, silver_df["passenger_count"] == passenger_dim["PASSENGER_COUNT"], "left") \
        .join(distance_dim, silver_df["trip_distance"] == distance_dim["TRIP_DISTANCE"], "left") \
        .join(datetime_dim, silver_df["pickup_datetime"] == datetime_dim["TPEP_PICKUP_DATETIME"], "left") \
        .join(location_dim.alias("pickup_loc"), silver_df["pickup_location_id"].cast("int") == col("pickup_loc.LOCATION_ID"), "left") \
        .join(location_dim.alias("dropoff_loc"), silver_df["dropoff_location_id"].cast("int") == col("dropoff_loc.LOCATION_ID"), "left")

    # Select and rename columns to match FACT_TAXI_RIDES
    fact_df = fact_df.select(
        silver_df["ride_id"],
        vendor_dim["VENDOR_ID"].alias("vendor_id"),
        datetime_dim["DATETIME_ID"].alias("datetime_id"),
        passenger_dim["PASSENGER_COUNT_ID"].alias("passenger_count_id"),
        distance_dim["TRIP_DISTANCE_ID"].alias("trip_distance_id"),
        rate_code_dim["RATE_CODE_ID"].alias("rate_code_id"),
        silver_df["store_and_fwd_flag"],
        col("pickup_loc.LOCATION_ID").alias("pickup_location_id"),
        col("dropoff_loc.LOCATION_ID").alias("dropoff_location_id"),
        payment_type_dim["PAYMENT_TYPE_ID"].alias("payment_type_id"),
        silver_df["fare_amount"],
        silver_df["extra"],
        silver_df["mta_tax"],
        silver_df["tip_amount"],
        silver_df["tolls_amount"],
        silver_df["improvement_surcharge"],
        silver_df["total_amount"],
        silver_df["congestion_surcharge"],
        silver_df["airport_fee"],
        silver_df["cbd_congestion_fee"],
        silver_df["ride_month"],
        silver_df["ride_type"],
        silver_df["ride_duration_minutes"],
        silver_df["avg_speed_mph"],
        silver_df["is_airport_trip"],
        silver_df["is_peak_hour"]
    )

    # Insert into FACT_TAXI_RIDES
    fact_table = session.table("FACT_TAXI_RIDES")
    fact_table.merge(
        source=fact_df,
        join_expr=fact_table["ride_id"] == fact_df["ride_id"],
        when_not_matched=[
            WhenNotMatchedClause(insert={col.name: fact_df[col.name] for col in fact_df.schema.fields})
        ]
    )

    # Log the load
    row_count = fact_df.count()
    session.sql("""
        INSERT INTO FACT_LOAD_LOG (
            fact_table_name, load_start_time, load_end_time, row_count, status, error_message
        )
        VALUES (?, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), ?, ?, ?)
    """).bind(["FACT_TAXI_RIDES", row_count, "success", None]).collect()

    return "Fact table successfully merged/updated."