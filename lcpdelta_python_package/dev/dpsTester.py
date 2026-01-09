import asyncio
from lcp_delta.enact.dps_helper import DPSHelper
from lcp_delta.enact.dps_helper_async import DPSHelperAsync
from lcp_delta.enact.dps_helper_new import DPSHelperNew

async def test_async(username, api_key):

    dps_helper_async = DPSHelperAsync(username, api_key)
    await dps_helper_async.start()
    def handle_updates(x):
        print(x)

    dps_helper_async.subscribe_to_series_updates(handle_updates,"RealtimeDemand", parse_datetimes=True)

    # keep alive
    message = None
    while message != "exit()":
        message = input(">> ")

    dps_helper_async.stop()


def test_new(username, api_key):
    dps_helper = DPSHelperNew(username, api_key)
    def handle_updates(x):
        print(x)

    dps_helper.subscribe_to_series_updates(handle_updates,"RealtimeDemand", parse_datetimes=True)
    # keep alive
    message = None
    while message != "exit()":
        message = input(">> ")

    dps_helper.terminate_hub_connection()

    
def test_original(username, api_key):
    dps_helper = DPSHelper(username, api_key)
    def handle_updates(x):
        print(x)

    dps_helper.subscribe_to_series_updates(handle_updates,"RealtimeDemand", parse_datetimes=True)
    # keep alive
    message = None
    while message != "exit()":
        message = input(">> ")

    dps_helper.terminate_hub_connection()


if __name__ == "__main__":
    username = "LcpInternalEnactAccessBaileyHalliday"
    api_key = "28AACrbX79aH"

    # test_original(username, api_key)

    asyncio.run(test_async(username, api_key))