from snowflake.snowpark import Session
from snowflake.snowpark.functions import (
    col, when_not_matched
)
from datetime import datetime

def fact_taxi_rides_ingest(session: Session) -> str:
    """
    Loads enriched taxi ride records from silver layer into FACT_TAXI_RIDES.
    Resolves surrogate keys from dimensions and inserts fact records.
    Returns:
        str: Status message indicating completion.
    """
    start_timestamp = datetime.now()

    try:
        silver_df = session.table("SILVER_NY_TAXI_RIDES")
        vendor_dim = session.table("VENDOR_DIM")
        rate_code_dim = session.table("RATE_CODE_DIM")
        payment_type_dim = session.table("PAYMENT_TYPE_DIM")
        passenger_dim = session.table("PASSENGER_COUNT_DIM")
        distance_dim = session.table("TRIP_DISTANCE_DIM")
        datetime_dim_pickup = session.table("DATETIME_DIM").alias("dt_pickup")
        datetime_dim_dropoff = session.table("DATETIME_DIM").alias("dt_dropoff")
        location_dim_pickup = session.table("LOCATION_DIM").alias("loc_pickup")
        location_dim_dropoff = session.table("LOCATION_DIM").alias("loc_pickup")
    except Exception as e:
        return f"Error reading source tables: {e}"

    # Resolve surrogate keys
    fact_df = silver_df \
    .join(vendor_dim, silver_df["VENDOR_ID"] == vendor_dim["VENDORID"], "left") \
    .join(rate_code_dim, silver_df["RATE_CODE_ID"].cast("int") == rate_code_dim["RATECODEID"], "left") \
    .join(payment_type_dim, silver_df["PAYMENT_TYPE"].cast("int") == payment_type_dim["PAYMENT_TYPE"], "left") \
    .join(passenger_dim, silver_df["PASSENGER_COUNT"] == passenger_dim["PASSENGER_COUNT"], "left") \
    .join(distance_dim, silver_df["TRIP_DISTANCE"] == distance_dim["TRIP_DISTANCE"], "left") \
    .join(datetime_dim_pickup, silver_df["PICKUP_DATETIME"] == datetime_dim_pickup["TPEP_PICKUP_DATETIME"], "left") \
    .join(datetime_dim_dropoff, silver_df["DROPOFF_DATETIME"] == datetime_dim_dropoff["TPEP_DROPOFF_DATETIME"], "left") \
    .join(location_dim_pickup, silver_df["PICKUP_LOCATION_ID"].cast("int") == location_dim_pickup["LOCATIONID"], "left") \
    .join(location_dim_dropoff, silver_df["DROPOFF_LOCATION_ID"].cast("int") == location_dim_dropoff["LOCATIONID"], "left")

    # Select and rename columns to match FACT_TAXI_RIDES
    fact_df = fact_df.select(
        silver_df["RIDE_ID"],
        vendor_dim["VENDOR_ID"].alias("VENDOR_ID"),
        datetime_dim_pickup["DATETIME_ID"].alias("PICKUP_DATETIME_ID"),
        datetime_dim_dropoff["DATETIME_ID"].alias("DROPOFF_DATETIME_ID"),
        passenger_dim["PASSENGER_COUNT_ID"].alias("PASSENGER_COUNT_ID"),
        distance_dim["TRIP_DISTANCE_ID"].alias("TRIP_DISTANCE_ID"),
        rate_code_dim["RATE_CODE_ID"].alias("RATE_CODE_ID"),
        silver_df["STORE_AND_FWD_FLAG"],
        location_dim_pickup["LOCATION_ID"].alias("PICKUP_LOCATION_ID"),
        location_dim_dropoff["LOCATION_ID"].alias("DROPOFF_LOCATION_ID"),
        payment_type_dim["PAYMENT_TYPE_ID"].alias("PAYMENT_TYPE_ID"),
        silver_df["FARE_AMOUNT"],
        silver_df["EXTRA"],
        silver_df["MTA_TAX"],
        silver_df["TIP_AMOUNT"],
        silver_df["TOLLS_AMOUNT"],
        silver_df["IMPROVEMENT_SURCHARGE"],
        silver_df["TOTAL_AMOUNT"],
        silver_df["CONGESTION_SURCHARGE"],
        silver_df["AIRPORT_FEE"],
        silver_df["CBD_CONGESTION_FEE"],
        silver_df["RIDE_MONTH"],
        silver_df["RIDE_TYPE"],
        silver_df["RIDE_DURATION_MINUTES"],
        silver_df["AVG_SPEED_MPH"],
        silver_df["IS_AIRPORT_TRIP"],
        silver_df["IS_PEAK_HOUR"]
    )

    # Insert into FACT_TAXI_RIDES
    fact_table = session.table("FACT_TAXI_RIDES")
    fact_table.merge(
        fact_df,
        fact_table["RIDE_ID"] == fact_df["RIDE_ID"],
        [
            when_not_matched().insert({col.name: fact_df[col.name] for col in fact_df.schema.fields})
        ]
    )
    
    # Log the load  
    end_timestamp = datetime.now()
    fact_table_name = "FACT_TAXI_RIDES"
    row_count = fact_df.count()
    
    session.sql(f"""
        INSERT INTO FACT_LOAD_LOG (
            fact_table_name, load_start_time, load_end_time, row_count, status, error_message
        )
        VALUES (
            '{fact_table_name}','{start_timestamp}', '{end_timestamp}', {row_count}, 'success', NULL
        )
    """).collect()

    return "Fact table successfully merged/updated."