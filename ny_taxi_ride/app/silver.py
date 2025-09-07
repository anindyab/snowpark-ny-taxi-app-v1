from __future__ import annotations
from datetime import datetime
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, when, lit, current_timestamp, hour, dayofweek, datediff, round


def silver_clean_procedure(session: Session) -> str:
        """
        Cleans and transforms raw taxi ride data from the bronze layer for the silver layer.
        - Filters out records with missing or invalid pickup/dropoff times, trip distance, or fare amount.
        - Computes derived metrics: ride duration (minutes), average speed (mph).
        - Flags data quality (good/outlier), airport trips, and peak hour rides.
        - Preserves audit columns (load_time, ride_id, file_name).
        - Writes cleaned and enriched data to SILVER_NY_TAXI_RIDES table (overwrites existing data).
        Returns:
            str: Status message indicating completion.
        Side Effects:
            - Overwrites SILVER_NY_TAXI_RIDES table with cleaned data.
        """
        
        start_timestamp = datetime.now()
        try:
            df = session.table("BRONZE_NY_TAXI_RIDES")

            # Base filters for valid records
            cleaned = (
                df.filter((col("PICKUP_DATETIME").is_not_null()) & (col("DROPOFF_DATETIME").is_not_null()))
                .filter(col("PICKUP_DATETIME") < col("DROPOFF_DATETIME"))
                .filter((col("TRIP_DISTANCE") > 0.1) & (col("FARE_AMOUNT") > 0))
            )

            # Derived metrics
            cleaned = cleaned.with_column(
                "RIDE_DURATION_MINUTES",
                datediff("minute", col("PICKUP_DATETIME"), col("DROPOFF_DATETIME"))
            )
            
            cleaned = cleaned.filter(col("RIDE_DURATION_MINUTES") > 0)
            
            cleaned = cleaned.with_column(
                "AVG_SPEED_MPH",
                when(
                    col("RIDE_DURATION_MINUTES") > 0,
                    round(col("TRIP_DISTANCE") / (col("RIDE_DURATION_MINUTES") / 60), 2)
                ).otherwise(lit(None))
            )

            # Quality flag
            cleaned = cleaned.with_column(
                "DATA_QUALITY_FLAG",
                when(
                    (col("RIDE_DURATION_MINUTES") > 0) & (col("AVG_SPEED_MPH") < 100),
                    lit("good")
                ).otherwise(lit("outlier"))
            )


            # Airport trip flag (simple heuristic based on zone name or location ID)
            cleaned = cleaned.with_column(
                "IS_AIRPORT_TRIP",
                when(
                    (col("PICKUP_LOCATION_ID").isin("132", "138")) |  # JFK, LaGuardia
                    (col("DROPOFF_LOCATION_ID").isin("132", "138")),
                    lit(True)
                ).otherwise(lit(False))
            )

            # Peak hour flag (e.g. 7–9 AM or 4–7 PM weekdays)
            cleaned = cleaned.with_column("PICKUP_HOUR", hour(col("PICKUP_DATETIME")))
            cleaned = cleaned.with_column("PICKUP_WEEKDAY", dayofweek(col("PICKUP_DATETIME")))

            cleaned = cleaned.with_column(
                "IS_PEAK_HOUR",
                when(
                    ((col("PICKUP_HOUR").between(7, 9)) | (col("PICKUP_HOUR").between(16, 19))) &
                    (col("PICKUP_WEEKDAY").between(2, 6)),  # Monday–Friday
                    lit(True)
                ).otherwise(lit(False))
            )

            # Preserve audit columns
            cleaned = (
                cleaned.with_column("LOAD_TIME", current_timestamp())
                .with_column("RIDE_ID", col("RIDE_ID"))
                .with_column("FILE_NAME", col("FILE_NAME"))
            )


            # Write to silver table
            cleaned.write.mode("overwrite").save_as_table("SILVER_NY_TAXI_RIDES")
        except Exception as e:
            raise ValueError(
                f"Error in silver transformation layer."
            ) from e

            # Process rejects based on quality checks
        try:
            df = df.with_column(
            "RIDE_DURATION_MINUTES",
            datediff("minute", col("PICKUP_DATETIME"), col("DROPOFF_DATETIME"))
            )

            df = df.with_column(
                "AVG_SPEED_MPH",
                when(
                    col("RIDE_DURATION_MINUTES") > 0,
                    round(col("TRIP_DISTANCE") / (col("RIDE_DURATION_MINUTES") / 60), 2)
                ).otherwise(lit(None))
            )

            # Process rejects based on quality checks
            reject_df = df.filter(
                (col("PICKUP_DATETIME") >= col("DROPOFF_DATETIME")) |
                (col("TRIP_DISTANCE") <= 0.1) | 
                (col("RIDE_DURATION_MINUTES") <= 0) |
                (col("AVG_SPEED_MPH") >= 100)
            )
            
            print(reject_df.count())
            
            reject_df = (
                reject_df.select(  # keep only the fields you care about
                    col("RIDE_ID"),
                    col("VENDOR_ID"),
                    col("PICKUP_DATETIME"),
                    col("DROPOFF_DATETIME"),
                    col("TRIP_DISTANCE"),
                    col("RIDE_DURATION_MINUTES"),
                    col("AVG_SPEED_MPH"),
                    lit("invalid_duration_or_speed").alias("REJECTION_REASON"),
                    lit("silver_quality_check").alias("REJECTION_STAGE"),
                    current_timestamp().alias("REJECTION_TIME")
                )
            )

            # Save rejects
            reject_df.write.mode("append").save_as_table("SILVER_NY_TAXI_RIDES_REJECTS")
    
        except Exception as e:
            raise ValueError(
                f"Error in silver reject layer."
            ) from e
        
        try:
            source_count = df.count()
            transformed_count = cleaned.count()
            rejected_count = source_count - transformed_count  # if you filtered out some rows
            end_timestamp = datetime.now()

            session.sql(f"""
                INSERT INTO SILVER_LOAD_LOG (
                    load_start_time, load_end_time, source_row_count, transformed_row_count,
                    rejected_row_count, status, error_message
                )
                VALUES (
                    TO_TIMESTAMP_NTZ('{start_timestamp}'),
                    TO_TIMESTAMP_NTZ('{end_timestamp}'),
                    {source_count},
                    {transformed_count},
                    {rejected_count},
                    'success',
                    NULL
                )
            """).collect()
        except Exception as e:
            session.sql(f"""
                INSERT INTO SILVER_LOAD_LOG (
                    load_start_time, load_end_time, source_row_count, transformed_row_count,
                    rejected_row_count, status, error_message
                )
                VALUES (
                    TO_TIMESTAMP_NTZ('{start_timestamp}'),
                    CURRENT_TIMESTAMP,
                    0, 0, 0,
                    'failed',
                    '{str(e)}'
                )
            """).collect()
            raise
        
        return "Silver transformation complete"