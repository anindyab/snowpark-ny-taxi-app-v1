from snowflake.snowpark import Session
from snowflake.snowpark.functions import (
    col, lit
)


def dim_payment_type_ingest(session: Session) -> str:
    """
    Ingests data from the silver layer and populates the PAYMENT_TYPE_DIM table.
    - Specifically, this procedure processes the PAYMENT_TYPE_DIM dimension.
    
    Returns:
        str: Status message indicating completion.
    """
    try:
        # Read the silver layer data
        silver_df = session.table("SILVER_NY_TAXI_RIDES")
    except Exception as e:
        return f"Error reading from silver layer: {e}"

    # Select unique payment types from the silver data
    unique_payment_types = silver_df.select(col("payment_type")) \
        .filter(col("payment_type").is_not_null()) \
        .filter(col("payment_type").rlike("^[0-9]+$")) \
        .distinct()


    # Add payment_type column using mapping
    payment_type_df = unique_payment_types.with_column(
        "payment_type_name",
        when(col("payment_type") == "0", lit("Flex Fare trip"))
        .when(col("payment_type") == "1", lit("Credit Card"))
        .when(col("payment_type") == "2", lit("Cash"))
        .when(col("payment_type") == "3", lit("No Charge"))
        .when(col("payment_type") == "4", lit("Dispute"))
        .when(col("payment_type") == "5", lit("Unknown"))
        .when(col("payment_type") == "6", lit("Voided Trip"))
        .otherwise(lit("Unknown"))
    )

    # Convert the column names to match the VENDOR_DIM table
    payment_type_df = payment_type_df.select(
        col("payment_type").cast("int").alias("PAYMENT_TYPE"),
        col("payment_type_name").alias("PAYMENT_TYPE_NAME")
    )

    # Merge the data into the PAYMENT_TYPE_DIM table
    payment_type_dim_table = session.table("PAYMENT_TYPE_DIM")
    from snowflake.snowpark import WhenMatchedClause, WhenNotMatchedClause

    payment_type_dim_table.merge(
        source=payment_type_df,
        join_expr=payment_type_dim_table["payment_type"] == payment_type_df["PAYMENT_TYPE"],
        when_matched=[
            WhenMatchedClause(update={"payment_type_name": payment_type_df["PAYMENT_TYPE_NAME"]})
        ],
        when_not_matched=[
            WhenNotMatchedClause(insert={"payment_type": payment_type_df["PAYMENT_TYPE"], "payment_type_name": payment_type_df["PAYMENT_TYPE_NAME"]})
        ]
    )
    
    row_count = payment_type_df.count()

    session.sql("""
        INSERT INTO DIMENSION_LOAD_LOG (
            dimension_name, load_start_time, load_end_time, row_count, status, error_message
        )
        VALUES (
            ?, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), ?, ?, ?
        )
    """).bind(["PAYMENT_TYPE_DIM", row_count, "success", None]).collect()
    
    return "Payment type Dimension table successfully merged/updated."


