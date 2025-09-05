from snowflake.snowpark import Session
from snowflake.snowpark.functions import (
    col, hour, dayofmonth, month, year, dayofweek, lit
)
from snowflake.snowpark import WhenMatchedClause, WhenNotMatchedClause

def dim_datetime_ingest(session: Session) -> str:
    """
    Ingests distinct pickup/dropoff timestamps from the silver layer and populates DATETIME_DIM.
    Adds temporal breakdowns and merges into the dimension table.
    Returns:
        str: Status message indicating completion.
    """

    try:
        silver_df = session.table("SILVER_NY_TAXI_RIDES")
    except Exception as e:
        return f"Error reading from silver layer: {e}"

    # Select distinct pickup/dropoff pairs
    datetime_df = silver_df.select(
        col("pickup_datetime").alias("TPEP_PICKUP_DATETIME"),
        col("dropoff_datetime").alias("TPEP_DROPOFF_DATETIME")
    ).distinct()

    # Add pickup breakdown
    datetime_df = datetime_df.with_columns({
        "PICK_HOUR": hour(col("TPEP_PICKUP_DATETIME")),
        "PICK_DAY": dayofmonth(col("TPEP_PICKUP_DATETIME")),
        "PICK_MONTH": month(col("TPEP_PICKUP_DATETIME")),
        "PICK_YEAR": year(col("TPEP_PICKUP_DATETIME")),
        "PICK_WEEKDAY": dayofweek(col("TPEP_PICKUP_DATETIME")),
        "PICKUP_DAY_NAME": dayofweek(col("TPEP_PICKUP_DATETIME"))  # Optional: map to name later
    })

    # Add dropoff breakdown
    datetime_df = datetime_df.with_columns({
        "DROP_HOUR": hour(col("TPEP_DROPOFF_DATETIME")),
        "DROP_DAY": dayofmonth(col("TPEP_DROPOFF_DATETIME")),
        "DROP_MONTH": month(col("TPEP_DROPOFF_DATETIME")),
        "DROP_YEAR": year(col("TPEP_DROPOFF_DATETIME")),
        "DROP_WEEKDAY": dayofweek(col("TPEP_DROPOFF_DATETIME")),
        "IS_WEEKEND": (dayofweek(col("TPEP_PICKUP_DATETIME")) >= lit(6)),
        "IS_PEAK_HOUR": (
            ((hour(col("TPEP_PICKUP_DATETIME")).between(7, 9)) |
             (hour(col("TPEP_PICKUP_DATETIME")).between(16, 19))) &
            (dayofweek(col("TPEP_PICKUP_DATETIME")).between(2, 6))
        )
    })

    # Merge into DATETIME_DIM
    dim_table = session.table("DATETIME_DIM")
    dim_table.merge(
        source=datetime_df,
        join_expr=dim_table["TPEP_PICKUP_DATETIME"] == datetime_df["TPEP_PICKUP_DATETIME"],
        when_matched=[
            WhenMatchedClause(update={
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
        ],
        when_not_matched=[
            WhenNotMatchedClause(insert={
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
    row_count = datetime_df.count()
    session.sql("""
        INSERT INTO DIMENSION_LOAD_LOG (
            dimension_name, load_start_time, load_end_time, row_count, status, error_message
        )
        VALUES (?, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), ?, ?, ?)
    """).bind(["DATETIME_DIM", row_count, "success", None]).collect()

    return "Datetime Dimension table successfully merged/updated."