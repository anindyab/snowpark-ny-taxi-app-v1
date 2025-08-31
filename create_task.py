import snowflake.connector
import os

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

    # Resume child first, then parent (order matters)
    cur.execute(resume_child_task_sql)
    print(f"▶️ Child task '{database_name}.{schema_name}.{child_task_name}' resumed.")

    cur.execute(resume_parent_task_sql)
    print(f"▶️ Parent task '{database_name}.{schema_name}.{parent_task_name}' resumed.")

except Exception as e:
    print(f"❌ Failed to create or resume tasks: {e}")

finally:
    cur.close()
    conn.close()
    print("🔒 Connection to Snowflake closed.")
