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
parent_task_name = "my_parent_task"
child_task_name = "my_child_task"
database_name = os.environ.get("SNOWFLAKE_DATABASE")
schema_name = "PUBLIC"

parent_task_sql = f"""
CREATE OR REPLACE TASK {database_name}.{schema_name}.{parent_task_name}
  WAREHOUSE = '{os.environ.get("SNOWFLAKE_WAREHOUSE")}'
  SCHEDULE = '1 MINUTE'
  COMMENT = 'Parent task in a DAG. Calls the test_procedure Snowpark stored procedure.'
AS
  CALL {schema_name}.test_procedure();
"""

child_task_sql = f"""
CREATE OR REPLACE TASK {database_name}.{schema_name}.{child_task_name}
  WAREHOUSE = '{os.environ.get("SNOWFLAKE_WAREHOUSE")}'
  COMMENT = 'Child task in a DAG. Calls the test_procedure_two Snowpark stored procedure.'
  AFTER {database_name}.{schema_name}.{parent_task_name}
AS
  CALL {schema_name}.test_procedure_two();
"""

resume_parent_task_sql = f"ALTER TASK {database_name}.{schema_name}.{parent_task_name} RESUME;"
resume_child_task_sql = f"ALTER TASK {database_name}.{schema_name}.{child_task_name} RESUME;"

try:
    cur = conn.cursor()
    
    # Set session context
    cur.execute(f"USE DATABASE {database_name};")
    cur.execute(f"USE SCHEMA {schema_name};")
    print(f"📌 Using database {database_name} and schema {schema_name}.")

    # Create tasks
    cur.execute(parent_task_sql)
    print(f"✅ Parent task '{database_name}.{schema_name}.{parent_task_name}' created.")

    cur.execute(child_task_sql)
    print(f"✅ Child task '{database_name}.{schema_name}.{child_task_name}' created.")

    # Resume child first, then parent
    cur.execute(resume_child_task_sql)
    print(f"▶️ Child task '{database_name}.{schema_name}.{child_task_name}' resumed.")

    cur.execute(resume_parent_task_sql)
    print(f"▶️ Parent task '{database_name}.{schema_name}.{parent_task_name}' resumed.")

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
    WHERE NAME IN ('{parent_task_name.upper()}', '{child_task_name.upper()}')
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
