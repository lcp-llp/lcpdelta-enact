import json
from datetime import datetime as dt

def single_series_test(dps_helper, file_path):

    def handle_updates(arg):
        entry = {
            "timestamp": dt.utcnow().isoformat(),
            "data": arg
        }

        with open(file_path, "a", encoding="utf-8") as f:
            json.dump(entry, f, default=str)
            f.write("\n")

    dps_helper.subscribe_to_series_updates(handle_updates,"ImbalancePriceRealtime", country_id="Belgium", parse_datetimes=True)

    # keep alive
    message = None
    while message != "exit()":
        message = input(">> ")

    dps_helper.terminate_hub_connection()

def multi_series_test(dps_helper, file_path):

    def handle_updates(arg):
        entry = {
            "timestamp": dt.utcnow().isoformat(),
            "data": arg
        }

        with open(file_path, "a", encoding="utf-8") as f:
            json.dump(entry, f, default=str)
            f.write("\n")

    multi_series_request = [
    # 1. System series with no options
    {"seriesId": "RealtimeFrequency", "countryId": "Gb"},

    # 2. Plant series subscriptions
    #    a) Two separate subscriptions:
    #       - All plants with fuel type "Battery"
    #       - All plants in zone "Z4"
    {"seriesId": "Mel", "optionIds": [["Battery"], ["Z4"]]},

    #    b) One subscription for all plants
    {"seriesId": "Mil", "optionIds": [["%ALL%"]]},

    #    c) One subscription for all plants owned by EDF
    {"seriesId": "Pn", "optionIds": [["EDF"]]},

    # 3. System series
    #    a) Two separate subscriptions:
    #       - CCGT generation outturn
    #       - Wind generation outturn
    {"seriesId": "OutturnFuel", "optionIds": [["CCGT"], ["Wind"]]},

    #    b) One subscription for all fuel types
    {"seriesId": "OutturnFuel", "optionIds": [["%ALL%"]]},

    # 4. System series that takes multiple-options
    #    a) Two seperate subscriptions:
    #				- One for wind at 10m, Zone 2, median aggregation
    #				- One for wind at 100m, Zone 5, median aggregation
    {"seriesId": "ERA5WindByZone", "optionIds": [["AboveGround10M", "Z2", "Median"],
                                                 ["AboveGround100M", "Z5", "Median"]]},

    #    b) One subscription for all heights, Zone 1, median aggregation
    {"seriesId": "ERA5WindByZone", "optionIds": [["%ALL%", "Z1", "Median"]]},

    #    c) One subscription for all heights, all zones, all aggregation types
    {"seriesId": "ERA5WindByZone", "optionIds": [["%ALL%", "%ALL%", "%ALL%"]]},
    ]   

    dps_helper.subscribe_to_multiple_series_updates(handle_updates, multi_series_request, parse_datetimes=True)

    # dps_helper_async.subscribe_to_series_updates(handle_updates,"ImbalancePriceRealtime", country_id="Belgium", parse_datetimes=True)
    # dps_helper_async.subscribe_to_series_updates(handle_updates,"RealTimeFrequency", parse_datetimes=True)
    # dps_helper_async.subscribe_to_series_updates(handle_updates,"RealTimeDemand", parse_datetimes=True)
    # keep alive
    message = None
    while message != "exit()":
        message = input(">> ")

    dps_helper.terminate_hub_connection()

def notification_test(dps_helper, file_path):

    def handle_updates(arg):
       # print(arg)
        entry = {
            "timestamp": dt.utcnow().isoformat(),
            "data": arg
        }

        with open(file_path, "a", encoding="utf-8") as f:
            json.dump(entry, f, default=str)
            f.write("\n")

    dps_helper.subscribe_to_notifications(handle_updates)

    message = None
    while message != "exit()":
        message = input(">> ")

    #Terminate the connection at the end
    dps_helper.terminate_hub_connection()

