from snowflake.snowpark import Session
from snowflake.snowpark.functions import (
    col, lit
)
from snowflake.snowpark import WhenMatchedClause, WhenNotMatchedClause
from snowflake.snowpark.functions import when

def dim_rate_code_ingest(session: Session) -> str:
    """
    Ingests distinct rate codes from the silver layer and populates RATE_CODE_DIM.
    Adds descriptive names and merges into the dimension table.
    Returns:
        str: Status message indicating completion.
    """

    try:
        silver_df = session.table("SILVER_NY_TAXI_RIDES")
    except Exception as e:
        return f"Error reading from silver layer: {e}"

    # Extract distinct rate codes
    rate_code_df = silver_df.select(col("rate_code_id")).filter(col("rate_code_id").is_not_null()).distinct()

    # Map rate_code_id to descriptive name
    rate_code_df = rate_code_df.with_column(
        "rate_code_name",
        when(col("rate_code_id") == "1", lit("Standard rate"))
        .when(col("rate_code_id") == "2", lit("JFK"))
        .when(col("rate_code_id") == "3", lit("Newark"))
        .when(col("rate_code_id") == "4", lit("Nassau or Westchester"))
        .when(col("rate_code_id") == "5", lit("Negotiated fare"))
        .when(col("rate_code_id") == "6", lit("Group ride"))
        .otherwise(lit("Other"))
    )

    # Cast and rename columns to match RATE_CODE_DIM
    rate_code_df = rate_code_df.select(
        col("rate_code_id").cast("int").alias("RATECODEID"),
        col("rate_code_name").alias("RATE_CODE_NAME")
    )

    # Merge into RATE_CODE_DIM
    rate_code_dim_table = session.table("RATE_CODE_DIM")

    rate_code_dim_table.merge(
        source=rate_code_df,
        join_expr=rate_code_dim_table["RATECODEID"] == rate_code_df["RATECODEID"],
        when_matched=[
            WhenMatchedClause(update={"RATE_CODE_NAME": rate_code_df["RATE_CODE_NAME"]})
        ],
        when_not_matched=[
            WhenNotMatchedClause(insert={
                "RATECODEID": rate_code_df["RATECODEID"],
                "RATE_CODE_NAME": rate_code_df["RATE_CODE_NAME"]
            })
        ]
    )

    # Log the load
    row_count = rate_code_df.count()
    session.sql("""
        INSERT INTO DIMENSION_LOAD_LOG (
            dimension_name, load_start_time, load_end_time, row_count, status, error_message
        )
        VALUES (?, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), ?, ?, ?)
    """).bind(["RATE_CODE_DIM", row_count, "success", None]).collect()

    return "Rate Code Dimension table successfully merged/updated."


