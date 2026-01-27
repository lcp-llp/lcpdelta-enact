from lcp_delta import enact
import time
import json
from datetime import timezone, datetime as dt

def log_to_file(arg, file_name):
    base_path = "C:\\Users\\Bailey.Halliday\\source\\repos\\lcpdelta-enact\\lcpdelta_python_package\\tests\\manual\\notification\\logs\\"
    file_path = base_path + file_name
    
    entry = {
            "timestamp": dt.now(timezone.utc).isoformat(),
            "data": arg
        }       
    with open(file_path, "a", encoding="utf-8") as f:
        json.dump(entry, f, default=str)
        f.write("\n")

def notification_test_sync(username, api_key):
    dps_helper_signalrcore = enact.DPSHelper(username, api_key)
    dps_helper_pysignalr = enact.DPSHelperPysignalr(username, api_key)

    def handle_updates_signalrcore(arg):  
        log_to_file(arg, "sync\\signalrcore.jsonl")

    def handle_updates_pysignalr(arg):  
        log_to_file(arg, "sync\\pysignalr.jsonl")

    dps_helper_signalrcore.subscribe_to_notifications(handle_updates_signalrcore)
    dps_helper_pysignalr.subscribe_to_notifications(handle_updates_pysignalr)

    message = None
    while message != "exit()":
        message = input(">> ")

    dps_helper_signalrcore.terminate_hub_connection()
    dps_helper_pysignalr.terminate_hub_connection()

def notification_test_async(username, api_key):
    dps_helper_signalrcore = enact.DPSHelper(username, api_key, True, 5)
    dps_helper_pysignalr = enact.DPSHelperPysignalr(username, api_key, True, 5)

    def handle_updates_signalrcore(arg):  
        log_to_file(arg, "async\\signalrcore.jsonl")

    def handle_updates_pysignalr(arg):  
        log_to_file(arg, "async\\pysignalr.jsonl")

    dps_helper_signalrcore.subscribe_to_notifications(handle_updates_signalrcore)
    dps_helper_pysignalr.subscribe_to_notifications(handle_updates_pysignalr)

    message = None
    while message != "exit()":
        message = input(">> ")

    dps_helper_signalrcore.terminate_hub_connection()
    dps_helper_pysignalr.terminate_hub_connection()


if __name__ == "__main__":

    username = "LcpInternalEnactAccessBaileyHalliday"
    api_key = "28AACrbX79aH"

    notification_test_async(username, api_key)
