from __future__ import annotations

from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, when, lit, current_timestamp, hour, dayofweek
from snowflake.snowpark import Session


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
        df = session.table("BRONZE_NY_TAXI_RIDES")

        # Base filters for valid records
        cleaned = (
            df.filter((col("pickup_datetime").is_not_null()) & (col("dropoff_datetime").is_not_null()))
            .filter(col("pickup_datetime") < col("dropoff_datetime"))
            .filter((col("trip_distance") > 0.1) & (col("fare_amount") > 0))
        )

        # Derived metrics
        cleaned = (
            cleaned.with_column("ride_duration_minutes", (col("dropoff_datetime") - col("pickup_datetime")) / 60)
            .with_column("avg_speed_mph", col("trip_distance") / (col("ride_duration_minutes") / 60))
        )

        # Quality flag
        cleaned = cleaned.with_column(
            "data_quality_flag",
            when(
                (col("ride_duration_minutes") > 0) &
                (col("avg_speed_mph") < 100),
                lit("good")
            ).otherwise(lit("outlier"))
        )

        # Airport trip flag (simple heuristic based on zone name or location ID)
        cleaned = cleaned.with_column(
            "is_airport_trip",
            when(
                (col("pickup_location_id").isin("132", "138")) |  # JFK, LaGuardia
                (col("dropoff_location_id").isin("132", "138")),
                lit(True)
            ).otherwise(lit(False))
        )

        # Peak hour flag (e.g. 7–9 AM or 4–7 PM weekdays)
        cleaned = cleaned.with_column("pickup_hour", hour(col("pickup_datetime")))
        cleaned = cleaned.with_column("pickup_weekday", dayofweek(col("pickup_datetime")))

        cleaned = cleaned.with_column(
            "is_peak_hour",
            when(
                ((col("pickup_hour").between(7, 9)) | (col("pickup_hour").between(16, 19))) &
                (col("pickup_weekday").between(2, 6)),  # Monday–Friday
                lit(True)
            ).otherwise(lit(False))
        )

        # Preserve audit columns
        cleaned = (
            cleaned.with_column("load_time", current_timestamp())
            .with_column("ride_id", col("ride_id"))
            .with_column("file_name", col("file_name"))
        )

        # Write to silver table
        cleaned.write.mode("overwrite").save_as_table("SILVER_NY_TAXI_RIDES")

        return "Silver transformation complete"

