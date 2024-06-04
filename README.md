# LCPDelta Python Package
This is the python wrapper to interact with all LCPDelta products through their API or DPS. To get started, install the latest version of the LCPDelta package.

To find out more about LCPDelta's data products, click [**here**][LCPDelta_data_portal_link].
To find out more about Enact, click [**here**][Enact_Homepage].

## Enact API and DPS Instructions

Full instructions on how to utilise Enact's full API and DPS can be found [**here**][Enact_instructions_link]. Below are some examples to get you started.

### Enact Series API Example Code

```python
from lcp_delta import enact
from datetime import date

username = "insert_username_here"
public_api_key = "insert_public_api_key_here"

api_helper = enact.APIHelper(username, public_api_key)

# Example dates
from_date= date(2022,4,1)
to_date = date(2023,5,31)

# Example series
series_id = "LcpDemandvsGrid"

response = api_helper.get_series_data(
    series_id,
    from_date,
    to_date,
    country_id = "Gb",
    time_zone_id="UTC"
)

print(response)
```

### Enact DPS Example Code

```python
from lcp_delta import enact

def handle_new_information(x):
    # A callback function that will be invoked with the received series updates.
    # The function should accept one argument, which will be the data received from the series updates.
    print(x)

username = "insert_username_here"
public_api_key = "insert_public_api_key_here"

dps_helper = enact.DPSHelper(username, public_api_key)
# Input method to handle any update to the series, alongside the series ID, that can be found on Enact.
dps_helper.subscribe_to_series_updates(handle_new_information, "RealtimeDemand")

message = None
while message != "exit()":
    message = input(">> ")

#Terminate the connection at the end
dps_helper.terminate_hub_connection()
```

### FLEXtrack API Example Code

```python
import lcp_delta.flextrack as flextrack
from datetime import datetime as dt

user = "insert_username_here"
api_key = "insert_public_api_key_here"

api_helper = flextrack.APIHelper(user, api_key)

response = api_helper.get_exporter_data(
    date_from=dt(2022, 11, 1),
    date_to=dt(2023, 10, 31),
    countries=['Austria'],
    products=["RegelleistungFcrProcuredFourHourly","RegelleistungFcrProcuredDaily","RegelleistungAfrrProcured"],
    directions=["Symmetric", "Upward", "Downward"],
    market='Availability',
    metrics=['Volume', 'Price'],
    aggregation_types=['Average', 'Average'],
    granularity='Monthly'
)

response.head
```

[Enact_instructions_link]: https://api.lcpdelta.com/
[LCPDelta_data_portal_link]: https://portal.lcpdelta.com/
[Enact_Homepage]: https://enact.lcpdelta.com/
[FLEXtrack_Homepage]: https://flextrack.lcpdelta.com/

# Contributing

Check out our [contribution guidelines](CONTRIBUTING.md) and [code of conduct](CODE_OF_CONDUCT.md).
