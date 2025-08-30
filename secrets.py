import os

def check_secret(secret_name: str):
    secret_value = os.getenv(secret_name)
    if secret_value:
        print(f"✅ Secret '{secret_name}' is available.")
        print(f"🔐 Value: {secret_value}")
    else:
        print(f"❌ Secret '{secret_name}' is not set or is empty.")

if __name__ == "__main__":
    check_secret("SNOWFLAKE_ACCOUNT")