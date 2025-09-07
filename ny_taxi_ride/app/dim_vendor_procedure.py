from snowflake.snowpark import Session
from snowflake.snowpark.functions import (
    col, when, lit, when_matched, when_not_matched
)
from datetime import datetime


def dim_vendor_ingest(session: Session) -> str:
    """
    Ingests data from the silver layer and populates the VENDOR_DIM table.
    - Specifically, this procedure processes the VENDOR_DIM dimension.
    
    Returns:
        str: Status message indicating completion.
    """
    start_timestamp = datetime.now()

    # Read the silver layer data
    silver_df = session.table("SILVER_NY_TAXI_RIDES")

    # Select unique vendor IDs from the silver data
    unique_vendor_ids = silver_df.select(col("VENDOR_ID")).filter(col("VENDOR_ID").is_not_null()).distinct()

    vendor_df = unique_vendor_ids.with_column(
        "VENDOR_NAME",
        when(col("VENDOR_ID") == lit(1), lit("Creative Mobile Technologies, LLC"))
        .when(col("VENDOR_ID") == lit(2), lit("Curb Mobility, LLC"))
        .when(col("VENDOR_ID") == lit(6), lit("Myle Technologies Inc"))
        .when(col("VENDOR_ID") == lit(7), lit("Helix"))
        .otherwise(lit("Unknown"))
    )
    

    # Merge the data into the VENDOR_DIM table
    vendor_dim_table = session.table("VENDOR_DIM")
    from snowflake.snowpark import WhenMatchedClause, WhenNotMatchedClause

    vendor_dim_table.merge(
        vendor_df,
        vendor_dim_table["VENDORID"] == vendor_df["VENDOR_ID"],
        [
            when_matched().update({"VENDOR_NAME": vendor_df["VENDOR_NAME"]}),
            when_not_matched().insert({"VENDORID": vendor_df["VENDOR_ID"], "VENDOR_NAME": vendor_df["VENDOR_NAME"]})
        ]
    )
    
    row_count = vendor_df.count()
    end_timestamp = datetime.now()
    dimension_name = "VENDOR_DIM"
    session.sql(f"""
        INSERT INTO DIMENSION_LOAD_LOG (
             dimension_name, load_start_time, load_end_time, row_count, status, error_message
        )
        VALUES (
            '{dimension_name}','{start_timestamp}', '{end_timestamp}', {row_count}, 'success', NULL
        )
    """).collect()
    
    return "Vendor Dimension table successfully merged/updated."

