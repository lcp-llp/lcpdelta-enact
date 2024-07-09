from dotenv import load_dotenv
import os

load_dotenv()

username = os.getenv("LCPDELTA_PACKAGE_TEST_USERNAME")
public_api_key = os.getenv("LCPDELTA_PACKAGE_TEST_API_KEY")

print(f"Loaded username: {username}")
print(f"Loaded public_api_key: {public_api_key}")
