"""
Collection of core functions for communicating with the Deutscher Wetterdienst (DWD) GeoServer.

The GeoServer is operated by the Deutscher Wetterdienst (DWD).
https://maps.dwd.de
"""

import urllib.parse
from typing import Any, Dict, Optional

import requests

# Default constants for WFS requests
DEFAULT_WFS_VERSION = "2.0.0"
DEFAULT_WFS_REQUEST = "GetFeature"
DEFAULT_WFS_OUTPUTFORMAT = "application/json"
DEFAULT_TIMEOUT = 10.0


def query_dwd(**kwargs: Any) -> Optional[Dict[str, Any]]:
    """
    Retrieve data from the DWD GeoServer using WFS (Web Feature Service).

    Args:
        **kwargs: Optional parameters for the WFS request, such as:
            - version: WFS version (default: 2.0.0)
            - request: WFS request type (default: GetFeature)
            - typename: The feature type name (required)
            - cql_filter: CQL filter for the query
            - outputformat: Output format (default: application/json)
            - timeout: Request timeout in seconds (default: 10.0)

    Returns:
        A JSON response as a dictionary if the request is successful, otherwise None.
    """
    # Normalize keys to lowercase and URL-encode values
    kwargs = {k.lower(): urllib.parse.quote(v) for k, v in kwargs.items()}

    # Ensure required 'typename' parameter is provided
    if "typename" not in kwargs:
        return None

    # Build the base query URL
    base_url = "https://maps.dwd.de/geoserver/dwd/ows?service=WFS"
    query_params = {
        "version": kwargs.get("version", DEFAULT_WFS_VERSION),
        "request": kwargs.get("request", DEFAULT_WFS_REQUEST),
        "typename": kwargs["typename"],
        "outputformat": kwargs.get("outputformat", DEFAULT_WFS_OUTPUTFORMAT),
    }

    # Add optional CQL filter if provided
    if "cql_filter" in kwargs:
        query_params["CQL_FILTER"] = kwargs["cql_filter"]

    # Construct the full query URL
    query = base_url + "&" + "&".join(f"{k}={v}" for k, v in query_params.items())

    # Set the timeout
    timeout = float(kwargs.get("timeout", DEFAULT_TIMEOUT))

    # Execute the request
    try:
        response = requests.get(query, timeout=timeout)
        response.raise_for_status()  # Raise an exception for HTTP errors
        return response.json()
    except (requests.RequestException, ValueError) as e:
        # Handle request errors or JSON decoding errors
        print(f"An error occurred: {e}")
        return None
