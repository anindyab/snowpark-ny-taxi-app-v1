import sys
import os
import yaml

# os.system(f"conda init")
# os.system(f"conda activate snowpark")

directory_path = sys.argv[1]

os.chdir(f"{directory_path}")

# This is where the flag is needed to allow shared libraries during package creation
os.system(f"snow snowpark build --allow-shared-libraries")

# The deploy command does not accept the --allow-shared-libraries flag
os.system(f"snow snowpark deploy --replace --temporary-connection --account $SNOWFLAKE_ACCOUNT --user $SNOWFLAKE_USER --role $SNOWFLAKE_ROLE --warehouse $SNOWFLAKE_WAREHOUSE --database $SNOWFLAKE_DATABASE")