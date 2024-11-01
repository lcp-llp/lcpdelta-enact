# LCPDelta Python Package
The LCPDelta Python package provides streamlined access to data available from the [**Enact**][Enact_Homepage], [**FLEXtrack**][FLEXtrack_Homepage] APIs.

It contains helper methods for requesting data from our API endpoints or subscribing to push groups, all of which are detailed on our [**developer documentation site**][Api_Docs].

### Installation
The Python package requires Python 3.10 or greater, and can be installed via:

```
pip install LCPDelta
```

### Usage

The Enact and FLEXtrack modules can be imported as follows:

```python
from lcp_delta import enact
import lcp_delta.flextrack as flextrack
```

The package requires a username and API key, which will be emailed on signup. Helper objects can then be instantiated as follows:
```python
username = "insert_username_here"
public_api_key = "insert_public_api_key_here"

enact_api_helper = enact.APIHelper(username, public_api_key)
enact_dps_helper = enact.DPSHelper(username, public_api_key)
flextrack_api_helper = flextrack.APIHelper(username, public_api_key)
```

From here, you can call any of the available helper methods to retrieve data in one call or listen for pushes. The following example makes use of Enact's Series Data endpoint:
```python

from_date = date(2023,10,1)
to_date = date(2024,10,1)

series_id = "LcpDemandvsGrid"

response = enact_api_helper.get_series_data(
    series_id,
    from_date,
    to_date,
    country_id = "Gb",
    time_zone_id="UTC"
)

print(response.head(5))
```

Check out our [**API guides**][Api_Docs] for detailed instructions on getting started with the API, our [**Recipes page**][Api_Recipes] for example scripts using our Python package, and our [**API reference**][Api_Reference] for details on each specific API endpoint (and corresponding Python package method).

Full documentation of our API and Python package can be found on our [**documentation site**][Api_Docs].

[Api_Docs]: https://api.lcpdelta.com/
[Api_Recipes]: https://api.lcpdelta.com/recipes
[Api_Reference]: https://api.lcpdelta.com/reference
[Enact_Homepage]: https://enact.lcpdelta.com/
[FLEXtrack_Homepage]: https://flextrack.lcpdelta.com/

# Contributing

Check out our [contribution guidelines](CONTRIBUTING.md) and [code of conduct](CODE_OF_CONDUCT.md).
