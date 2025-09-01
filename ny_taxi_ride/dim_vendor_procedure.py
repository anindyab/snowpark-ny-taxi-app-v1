from snowflake.snowpark import Session
from snowflake.snowpark.functions import (
    col, hour, dayofmonth, month, year, dayofweek, lit, current_timestamp, udf
)
from snowflake.snowpark import WhenMatchedClause, WhenNotMatchedClause

def dim_vendor_ingest(session: Session) -> str:
    """
    Ingests data from the silver layer and populates the VENDOR_DIM table.
    - Specifically, this procedure processes the VENDOR_DIM dimension.
    
    Returns:
        str: Status message indicating completion.
    """
    try:
        # Read the silver layer data
        silver_df = session.table("SILVER_NY_TAXI_RIDES")
    except Exception as e:
        return f"Error reading from silver layer: {e}"

    # Select unique vendor IDs from the silver data
    unique_vendor_ids = silver_df.select(col("vendor_id")).filter(col("vendor_id").is_not_null()).distinct()

    # Map vendor ID to a descriptive name
    vendor_map = {
        "1": "Creative Mobile Technologies, LLC",
        "2": "Curb Mobility, LLC",
        "6": "Myle Technologies Inc",
        "7": "Helix"
    }

    # Add vendor_name column using mapping
    def get_vendor_name(vendor_id):
        return vendor_map.get(str(vendor_id), "Unknown")

    get_vendor_name_udf = udf(get_vendor_name, return_type="string")

    vendor_df = unique_vendor_ids.with_column(
        "vendor_name",
        get_vendor_name_udf(col("vendor_id"))
    )

    # Convert the column names to match the VENDOR_DIM table
    vendor_df = vendor_df.select(
        col("vendor_id").cast("int").alias("VENDOR_ID"),
        col("vendor_name").alias("vendor")
    )

    # Merge the data into the VENDOR_DIM table
    vendor_dim_table = session.table("VENDOR_DIM")
    from snowflake.snowpark import WhenMatchedClause, WhenNotMatchedClause

    vendor_dim_table.merge(
        source=vendor_df,
        join_expr=vendor_dim_table["vendor_id"] == vendor_df["VENDOR_ID"],
        when_matched=[
            WhenMatchedClause(update={"vendor": vendor_df["vendor"]})
        ],
        when_not_matched=[
            WhenNotMatchedClause(insert={"vendor_id": vendor_df["VENDOR_ID"], "vendor": vendor_df["vendor"]})
        ]
    )
    
    row_count = vendor_df.count()

    session.sql("""
        INSERT INTO DIMENSION_LOAD_LOG (
            dimension_name, load_start_time, load_end_time, row_count, status, error_message
        )
        VALUES (
            ?, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), ?, ?, ?
        )
    """).bind(["VENDOR_DIM", row_count, "success", None]).collect()
    
    return "Vendor Dimension table successfully merged/updated."

