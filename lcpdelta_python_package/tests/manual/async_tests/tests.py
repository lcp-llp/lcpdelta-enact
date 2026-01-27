from lcp_delta import enact
import time
import json
from datetime import timezone, datetime as dt

def log_to_file(arg, file_name):
    base_path = "C:\\Users\\Bailey.Halliday\\source\\repos\\lcpdelta-enact\\lcpdelta_python_package\\tests\\manual\\async_mode_logs\\"
    file_path = base_path + file_name
    
    entry = {
            "timestamp": dt.now(timezone.utc).isoformat(),
            "data": arg
        }       
    with open(file_path, "a", encoding="utf-8") as f:
        json.dump(entry, f, default=str)
        f.write("\n")

def test1(username, api_key):
    dps_helper_signalrcore = enact.DPSHelper(username, api_key, True, 5)
    dps_helper_pysignalr = enact.DPSHelperPysignalr(username, api_key, True, 5)

    test_id = "test1"

    def handle_updates_signalrcore(arg):  
        log_to_file(arg, test_id +"\\signalrcore.jsonl")

    def handle_updates_pysignalr(arg):  
        log_to_file(arg, test_id +"\\pysignalr.jsonl")

    multi_series_request = [
    {"seriesId": "RealtimeFrequency", "countryId": "Gb"},
    {"seriesId": "RealtimeDemand", "countryId": "Gb"},
    {"seriesId": "ImbalancePriceRealtime", "countryId": "Belgium"},
    ]  
    
    start_time = time.perf_counter()
    dps_helper_signalrcore.subscribe_to_multiple_series_updates(handle_updates_signalrcore, multi_series_request)
    dps_helper_pysignalr.subscribe_to_multiple_series_updates(handle_updates_pysignalr, multi_series_request)

    # keep alive
    message = None
    while message != "exit()":
        message = input(">> ")
    end_time = time.perf_counter()

    dps_helper_signalrcore.terminate_hub_connection()
    dps_helper_pysignalr.terminate_hub_connection()

    execution_time = end_time - start_time
    print(f"Execution time: {execution_time:.6f} seconds")

def test2(username, api_key):
    dps_helper_signalrcore = enact.DPSHelper(username, api_key, True, 1)
    dps_helper_pysignalr = enact.DPSHelperPysignalr(username, api_key, True, 1)

    test_id = "test2"

    def handle_updates_signalrcore(arg):  
        log_to_file(arg, test_id +"\\signalrcore.jsonl")

    def handle_updates_pysignalr(arg):  
        log_to_file(arg, test_id +"\\pysignalr.jsonl")

    multi_series_request = [
    {"seriesId": "RealtimeFrequency", "countryId": "Gb"},
    {"seriesId": "RealtimeDemand", "countryId": "Gb"},
    {"seriesId": "ImbalancePriceRealtime", "countryId": "Belgium"},
    ]  
    
    start_time = time.perf_counter()
    dps_helper_signalrcore.subscribe_to_multiple_series_updates(handle_updates_signalrcore, multi_series_request)
    dps_helper_pysignalr.subscribe_to_multiple_series_updates(handle_updates_pysignalr, multi_series_request)

    # keep alive
    message = None
    while message != "exit()":
        message = input(">> ")
    end_time = time.perf_counter()

    dps_helper_signalrcore.terminate_hub_connection()
    dps_helper_pysignalr.terminate_hub_connection()

    execution_time = end_time - start_time
    print(f"Execution time: {execution_time:.6f} seconds")

def test3(username, api_key):
    dps_helper_signalrcore = enact.DPSHelper(username, api_key, True, 3)
    dps_helper_pysignalr = enact.DPSHelperPysignalr(username, api_key, True, 3)

    test_id = "test3"

    def handle_updates_signalrcore(arg):  
        log_to_file(arg, test_id +"\\signalrcore.jsonl")

    def handle_updates_pysignalr(arg):  
        log_to_file(arg, test_id +"\\pysignalr.jsonl")

    multi_series_request = [
    {"seriesId": "RealtimeFrequency", "countryId": "Gb"},
    {"seriesId": "RealtimeDemand", "countryId": "Gb"},
    {"seriesId": "ImbalancePriceRealtime", "countryId": "Belgium"},
    ]  
    
    start_time = time.perf_counter()
    dps_helper_signalrcore.subscribe_to_multiple_series_updates(handle_updates_signalrcore, multi_series_request)
    dps_helper_pysignalr.subscribe_to_multiple_series_updates(handle_updates_pysignalr, multi_series_request)

    # keep alive
    message = None
    while message != "exit()":
        message = input(">> ")
    end_time = time.perf_counter()

    dps_helper_signalrcore.terminate_hub_connection()
    dps_helper_pysignalr.terminate_hub_connection()

    execution_time = end_time - start_time
    print(f"Execution time: {execution_time:.6f} seconds")

if __name__ == "__main__":
    
    username = "LcpInternalEnactAccessBaileyHalliday"
    api_key = "28AACrbX79aH"

    test3(username, api_key)

   