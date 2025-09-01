import snowflake.connector
import os
import time

# --- Print environment variables for debugging ---
print(os.environ.get("SNOWFLAKE_ACCOUNT"))
print(os.environ.get("SNOWFLAKE_USER"))
print(os.environ.get("SNOWFLAKE_PASSWORD"))
print(os.environ.get("SNOWFLAKE_WAREHOUSE"))
print(os.environ.get("SNOWFLAKE_DATABASE"))
print(os.environ.get("SNOWFLAKE_SCHEMA"))
print(os.environ.get("SNOWFLAKE_ROLE"))

# --- Establish Connection to Snowflake ---
try:
    conn = snowflake.connector.connect(
        account=os.environ.get("SNOWFLAKE_ACCOUNT"),
        user=os.environ.get("SNOWFLAKE_USER"),
        password=os.environ.get("SNOWFLAKE_PASSWORD"),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE"),
        database=os.environ.get("SNOWFLAKE_DATABASE"),
        role=os.environ.get("SNOWFLAKE_ROLE"),
        schema=os.environ.get("SNOWFLAKE_SCHEMA")
    )
except snowflake.connector.errors.ProgrammingError as e:
    print(f"Error connecting to Snowflake: {e}")
    exit(1)

print("✅ Successfully connected to Snowflake.")

# --- Task Definitions ---
nightly_root_task_name = "NIGHTLY_ROOT_TASK"
silver_clean_task_name = "SILVER_CLEAN_TASK"
gold_model_task_name = "GOLD_MODEL_TASK"
database_name = os.environ.get("SNOWFLAKE_DATABASE")
schema_name = os.environ.get("SNOWFLAKE_SCHEMA")

nightly_root_task_sql = f"""
CREATE OR REPLACE TASK {database_name}.{schema_name}.{nightly_root_task_name}
  WAREHOUSE = '{os.environ.get("SNOWFLAKE_WAREHOUSE")}'
  SCHEDULE = 'USING CRON 0 2 * * * UTC'
  COMMENT = 'Nightly root task in the DAG. Calls the bronze_ingest_procedure Snowpark stored procedure.'
AS
  CALL {schema_name}.bronze_ingest_procedure();
"""

silver_clean_task_sql = f"""
CREATE OR REPLACE TASK {database_name}.{schema_name}.{silver_clean_task_name}
  WAREHOUSE = '{os.environ.get("SNOWFLAKE_WAREHOUSE")}'
  COMMENT = 'Silver clean task in the DAG. Calls the silver_clean_procedure Snowpark stored procedure.'
  AFTER {database_name}.{schema_name}.{nightly_root_task_name}
AS
  CALL {schema_name}.silver_clean_procedure();
"""

gold_model_task_sql = f"""
CREATE OR REPLACE TASK {database_name}.{schema_name}.{gold_model_task_name}
  WAREHOUSE = '{os.environ.get("SNOWFLAKE_WAREHOUSE")}'
  COMMENT = 'Silver clean task in the DAG. Calls the gold_model_procedure Snowpark stored procedure.'
  AFTER {database_name}.{schema_name}.{silver_clean_task_name}
AS
  CALL {schema_name}.gold_model_procedure();
"""

resume_nightly_root_task_sql = f"ALTER TASK {database_name}.{schema_name}.{nightly_root_task_name} RESUME;"
resume_silver_clean_task_sql = f"ALTER TASK {database_name}.{schema_name}.{silver_clean_task_name} RESUME;"
resume_gold_model_task_sql = f"ALTER TASK {database_name}.{schema_name}.{gold_model_task_name} RESUME;"

try:
    cur = conn.cursor()
    
    # Set session context
    cur.execute(f"USE DATABASE {database_name};")
    cur.execute(f"USE SCHEMA {schema_name};")
    print(f"📌 Using database {database_name} and schema {schema_name}.")

    # Create tasks
    cur.execute(nightly_root_task_sql)
    print(f"✅ Nightly root task '{database_name}.{schema_name}.{nightly_root_task_name}' created.")

    cur.execute(silver_clean_task_sql)
    print(f"✅ Silver clean task '{database_name}.{schema_name}.{silver_clean_task_name}' created.")
    
    cur.execute(gold_model_task_sql)
    print(f"✅ Gold model task '{database_name}.{schema_name}.{gold_model_task_name}' created.")

    # Resume child first, then parent
    
    cur.execute(resume_gold_model_task_sql)
    print(f"▶️ Gold model task '{database_name}.{schema_name}.{gold_model_task_name}' resumed.")

    cur.execute(resume_silver_clean_task_sql)
    print(f"▶️ Silver clean task '{database_name}.{schema_name}.{silver_clean_task_name}' resumed.")

    cur.execute(resume_nightly_root_task_sql)
    print(f"▶️ Parent task '{database_name}.{schema_name}.{nightly_root_task_name}' resumed.")



    # --- Wait for tasks to run at least once ---
    print("⏳ Waiting 70 seconds for tasks to execute...")
    time.sleep(70)  # Wait slightly longer than the 1-minute schedule

    # --- Query task history ---
    history_sql = f"""
    SELECT 
      NAME,
      STATE,
      SCHEDULED_TIME,
      COMPLETED_TIME,
      QUERY_ID,
      ERROR_MESSAGE
    FROM SNOWFLAKE.ACCOUNT_USAGE.TASK_HISTORY
    WHERE NAME IN ('{nightly_root_task_name.upper()}', '{silver_clean_task_name.upper()}', '{gold_model_task_name.upper()}')
      AND SCHEDULED_TIME >= DATEADD('minute', -5, CURRENT_TIMESTAMP())
    ORDER BY SCHEDULED_TIME DESC;
    """
    cur.execute(history_sql)
    rows = cur.fetchall()

    print("\n📊 Task Execution History (last 5 minutes):")
    for row in rows:
        print(row)

except Exception as e:
    print(f"❌ Failed to create or check tasks: {e}")

finally:
    cur.close()
    conn.close()
    print("🔒 Connection to Snowflake closed.")
