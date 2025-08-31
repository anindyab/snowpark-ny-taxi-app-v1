import snowflake.connector
import os

print(os.environ.get("SNOWFLAKE_ACCOUNT"))
print(os.environ.get("SNOWFLAKE_USER"))
print(os.environ.get("SNOWFLAKE_PASSWORD"))
print(os.environ.get("SNOWFLAKE_WAREHOUSE"))
print(os.environ.get("SNOWFLAKE_DATABASE"))
print(os.environ.get("SNOWFLAKE_SCHEMA"))
print(os.environ.get("SNOWFLAKE_ROLE"))

# --- Establish Connection to Snowflake ---
# The script reads connection details from environment variables.
# This ensures sensitive information is not hardcoded.
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

print("Successfully connected to Snowflake.")

# --- Define and Execute the CREATE TASK SQL Command ---
# Define task parameters using environment variables for a fully qualified name
task_name = "my_test_procedure_task"
database_name = os.environ.get("SNOWFLAKE_DATABASE")
schema_name = "PUBLIC"


# This SQL statement creates a task with a fully qualified name
task_sql = f"""
CREATE OR REPLACE TASK {database_name}.{schema_name}.{task_name}
  WAREHOUSE = '{os.environ.get("SNOWFLAKE_WAREHOUSE")}'
  SCHEDULE = 'USING CRON 0 4 * * * UTC'
  COMMENT = 'Calls the test_procedure Snowpark stored procedure.'
AS
  CALL {schema_name}.test_procedure();
"""

try:
    cur = conn.cursor()
    
    # Set the session context explicitly by executing two separate statements.
    cur.execute(f"USE DATABASE {database_name};")
    cur.execute(f"USE SCHEMA {schema_name};")
    print(f"Using database {database_name} and schema {schema_name}.")

    # Execute the CREATE TASK command with a fully qualified name
    cur.execute(task_sql)
    print(f"Task '{database_name}.{schema_name}.{task_name}' created successfully.")

except Exception as e:
    print(f"Failed to create task: {e}")

finally:
    # --- Clean up resources ---
    cur.close()
    conn.close()
    print("Connection to Snowflake closed.")
