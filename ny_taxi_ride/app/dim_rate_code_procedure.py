from snowflake.snowpark import Session
from snowflake.snowpark.functions import (
    col, lit, when_matched, when_not_matched
)
from datetime import datetime

from snowflake.snowpark.functions import when

def dim_rate_code_ingest(session: Session) -> str:
    """
    Ingests distinct rate codes from the silver layer and populates RATE_CODE_DIM.
    Adds descriptive names and merges into the dimension table.
    Returns:
        str: Status message indicating completion.
    """
    start_timestamp = datetime.now()

    try:
        silver_df = session.table("SILVER_NY_TAXI_RIDES")
    except Exception as e:
        return f"Error reading from silver layer: {e}"

    # Extract distinct rate codes
    rate_code_df = silver_df.select(col("RATE_CODE_ID")).filter(col("RATE_CODE_ID").is_not_null()).distinct()

    # Map rate_code_id to descriptive name
    rate_code_df = rate_code_df.with_column(
        "RATE_CODE_NAME",
        when(col("RATE_CODE_ID") == lit(1), lit("Standard rate"))
        .when(col("RATE_CODE_ID") == lit(2), lit("JFK"))
        .when(col("RATE_CODE_ID") == lit(3), lit("Newark"))
        .when(col("RATE_CODE_ID") == lit(4), lit("Nassau or Westchester"))
        .when(col("RATE_CODE_ID") == lit(5), lit("Negotiated fare"))
        .when(col("RATE_CODE_ID") == lit(6), lit("Group ride"))
        .otherwise(lit("Other"))
    )
    # Cast and rename columns to match RATE_CODE_DIM
    rate_code_df = rate_code_df.select(
        col("RATE_CODE_ID").alias("RATECODEID"),
        col("RATE_CODE_NAME").alias("RATE_CODE_NAME")
    )

    # Merge into RATE_CODE_DIM
    rate_code_dim_table = session.table("RATE_CODE_DIM")

    rate_code_dim_table.merge(
        rate_code_df,
        rate_code_dim_table["RATECODEID"] == rate_code_df["RATECODEID"],
        [
            when_matched().update({"RATE_CODE_NAME": rate_code_df["RATE_CODE_NAME"]}),
            when_not_matched().insert({
                "RATECODEID": rate_code_df["RATECODEID"],
                "RATE_CODE_NAME": rate_code_df["RATE_CODE_NAME"]
            })
        ]
    )

    # Log the load
    end_timestamp = datetime.now()
    dimension_name = "RATE_CODE_DIM"
    row_count = rate_code_df.count()
    session.sql(f"""
        INSERT INTO DIMENSION_LOAD_LOG (
             dimension_name, load_start_time, load_end_time, row_count, status, error_message
        )
        VALUES (
            '{dimension_name}','{start_timestamp}', '{end_timestamp}', {row_count}, 'success', NULL
        )
    """).collect()

    return "Rate Code Dimension table successfully merged/updated."


