from dotenv import load_dotenv
import os
from lcp_delta import enact, flextrack

load_dotenv()

enact_username = os.getenv("INTEGRATION_TEST_ENACT_USERNAME")
enact_public_api_key = os.getenv("INTEGRATION_TEST_ENACT_API_KEY")

flextrack_username = os.getenv("INTEGRATION_TEST_FLEXTRACK_USERNAME")
flextrack_public_api_key = os.getenv("INTEGRATION_TEST_FLEXTRACK_API_KEY")

enact_api_helper = enact.APIHelper(enact_username, enact_public_api_key)
flextrack_api_helper = flextrack.APIHelper(flextrack_username, flextrack_public_api_key)
