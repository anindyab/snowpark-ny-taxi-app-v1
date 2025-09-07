from snowflake.snowpark import Session
from snowflake.snowpark.functions import (
    col, hour, dayofmonth, month, year, dayofweek, lit, when_matched, when_not_matched
)
from datetime import datetime

def dim_datetime_ingest(session: Session) -> str:
    """
    Ingests distinct pickup/dropoff timestamps from the silver layer and populates DATETIME_DIM.
    Adds temporal breakdowns and merges into the dimension table.
    Returns:
        str: Status message indicating completion.
    """
    start_timestamp = datetime.now()

    try:
        silver_df = session.table("SILVER_NY_TAXI_RIDES")
    except Exception as e:
        return f"Error reading from silver layer: {e}"

    # Select distinct pickup/dropoff pairs
    datetime_df = silver_df.select(
        col("PICKUP_DATETIME").alias("TPEP_PICKUP_DATETIME"),
        col("DROPOFF_DATETIME").alias("TPEP_DROPOFF_DATETIME")
    ).distinct()

    # Add pickup breakdown
    datetime_df = datetime_df.with_columns(
        ["PICK_HOUR", "PICK_DAY", "PICK_MONTH", "PICK_YEAR", "PICK_WEEKDAY", "PICKUP_DAY_NAME"],
        [
            hour(col("TPEP_PICKUP_DATETIME")),
            dayofmonth(col("TPEP_PICKUP_DATETIME")),
            month(col("TPEP_PICKUP_DATETIME")),
            year(col("TPEP_PICKUP_DATETIME")),
            dayofweek(col("TPEP_PICKUP_DATETIME")),
            dayofweek(col("TPEP_PICKUP_DATETIME"))  # (later replace with name mapping if needed)
        ]
    )

    # Add dropoff breakdown
    datetime_df = datetime_df.with_columns(
        ["DROP_HOUR", "DROP_DAY", "DROP_MONTH", "DROP_YEAR", "DROP_WEEKDAY", "IS_WEEKEND", "IS_PEAK_HOUR"],
        [
            hour(col("TPEP_DROPOFF_DATETIME")),
            dayofmonth(col("TPEP_DROPOFF_DATETIME")),
            month(col("TPEP_DROPOFF_DATETIME")),
            year(col("TPEP_DROPOFF_DATETIME")),
            dayofweek(col("TPEP_DROPOFF_DATETIME")),
            (dayofweek(col("TPEP_PICKUP_DATETIME")) >= lit(6)),
            (
                ((hour(col("TPEP_PICKUP_DATETIME")).between(7, 9)) |
                (hour(col("TPEP_PICKUP_DATETIME")).between(16, 19)))
                & (dayofweek(col("TPEP_PICKUP_DATETIME")).between(2, 6))
            )
        ]
    )
    
    # Merge into DATETIME_DIM
    dim_table = session.table("DATETIME_DIM")
    dim_table.merge(
        datetime_df,
        ((dim_table["TPEP_PICKUP_DATETIME"] == datetime_df["TPEP_PICKUP_DATETIME"]) &
        (dim_table["TPEP_DROPOFF_DATETIME"] == datetime_df["TPEP_DROPOFF_DATETIME"])),
        [
            when_matched().update({
                "PICK_HOUR": datetime_df["PICK_HOUR"],
                "PICK_DAY": datetime_df["PICK_DAY"],
                "PICK_MONTH": datetime_df["PICK_MONTH"],
                "PICK_YEAR": datetime_df["PICK_YEAR"],
                "PICK_WEEKDAY": datetime_df["PICK_WEEKDAY"],
                "DROP_HOUR": datetime_df["DROP_HOUR"],
                "DROP_DAY": datetime_df["DROP_DAY"],
                "DROP_MONTH": datetime_df["DROP_MONTH"],
                "DROP_YEAR": datetime_df["DROP_YEAR"],
                "DROP_WEEKDAY": datetime_df["DROP_WEEKDAY"],
                "IS_WEEKEND": datetime_df["IS_WEEKEND"],
                "IS_PEAK_HOUR": datetime_df["IS_PEAK_HOUR"],
                "PICKUP_DAY_NAME": datetime_df["PICKUP_DAY_NAME"]
            }),
            when_not_matched().insert({
                "TPEP_PICKUP_DATETIME": datetime_df["TPEP_PICKUP_DATETIME"],
                "TPEP_DROPOFF_DATETIME": datetime_df["TPEP_DROPOFF_DATETIME"],
                "PICK_HOUR": datetime_df["PICK_HOUR"],
                "PICK_DAY": datetime_df["PICK_DAY"],
                "PICK_MONTH": datetime_df["PICK_MONTH"],
                "PICK_YEAR": datetime_df["PICK_YEAR"],
                "PICK_WEEKDAY": datetime_df["PICK_WEEKDAY"],
                "DROP_HOUR": datetime_df["DROP_HOUR"],
                "DROP_DAY": datetime_df["DROP_DAY"],
                "DROP_MONTH": datetime_df["DROP_MONTH"],
                "DROP_YEAR": datetime_df["DROP_YEAR"],
                "DROP_WEEKDAY": datetime_df["DROP_WEEKDAY"],
                "IS_WEEKEND": datetime_df["IS_WEEKEND"],
                "IS_PEAK_HOUR": datetime_df["IS_PEAK_HOUR"],
                "PICKUP_DAY_NAME": datetime_df["PICKUP_DAY_NAME"]
            })
        ]
    )

    # Log the load 
    end_timestamp = datetime.now()
    dimension_name = "DATETIME_DIM"
    row_count = datetime_df.count()  
    
    session.sql(f"""
        INSERT INTO DIMENSION_LOAD_LOG (
             dimension_name, load_start_time, load_end_time, row_count, status, error_message
        )
        VALUES (
            '{dimension_name}','{start_timestamp}', '{end_timestamp}', {row_count}, 'success', NULL
        )
    """).collect()

    return "Datetime Dimension table successfully merged/updated."