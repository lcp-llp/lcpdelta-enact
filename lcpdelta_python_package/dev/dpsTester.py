import asyncio
from lcp_delta.enact.dps_helper import DPSHelper
#from lcp_delta.enact.dps_helper_async import DPSHelperAsync
#from lcp_delta.enact.dps_helper_new import DPSHelperNew

from lcp_delta.enact.dps_helper_asyncV2 import DPSHelperAsyncV2
from lcp_delta.enact.dps_helper_asyncV3 import DPSHelperAsyncV3

# async def test_asyncv2(username, api_key):

#     dps_helper_async = DPSHelperAsyncV2(username, api_key)
#     await dps_helper_async.start()

def test_asyncv3(username, api_key):
    def handle_updates(df):
        print(df)

    dps_helper_async = DPSHelperAsyncV3(username, api_key)
    dps_helper_async.subscribe_to_series_updates(handle_updates,"ImbalancePriceRealtime", country_id="Belgium", parse_datetimes=True)
    
    message = None
    while message != "exit()":
        message = input(">> ")

    dps_helper_async.terminate_hub_connection()



# async def test_async(username, api_key):
#     def handle_updates(x):
#             print(x)

#     dps_helper_async = DPSHelperAsync(username, api_key)
#     dps_helper_async.subscribe_to_series_updates(handle_updates,"RealtimeDemand", parse_datetimes=True)

#     await dps_helper_async.start()




# def test_new(username, api_key):
#     dps_helper = DPSHelperNew(username, api_key)
#     def handle_updates(x):
#         print(x)

#     dps_helper.subscribe_to_series_updates(handle_updates,"RealtimeDemand", parse_datetimes=True)
#     # keep alive
#     message = None
#     while message != "exit()":
#         message = input(">> ")

#     dps_helper.terminate_hub_connection()

    
def test_original(username, api_key):
    dps_helper = DPSHelper(username, api_key)
    def handle_updates(x):
        print(x)

    dps_helper.subscribe_to_series_updates(handle_updates,"ImbalancePriceRealtime", country_id="Belgium", parse_datetimes=True)
    # keep alive
    message = None
    while message != "exit()":
        message = input(">> ")

    dps_helper.terminate_hub_connection()


if __name__ == "__main__":
    username = "LcpInternalEnactAccessBaileyHalliday"
    api_key = "28AACrbX79aH"

    test_original(username, api_key)
    
    # test_asyncv3(username, api_key)