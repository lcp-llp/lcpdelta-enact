from dotenv import load_dotenv
import os
from lcp_delta import enact

load_dotenv()

username = os.getenv("LCPDELTA_PACKAGE_TEST_USERNAME")
public_api_key = os.getenv("LCPDELTA_PACKAGE_TEST_API_KEY")

enact_api_helper = enact.APIHelper(username, public_api_key)
