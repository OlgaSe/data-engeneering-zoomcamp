"""Template for building a `dlt` pipeline to ingest data from a REST API."""

import dlt
from dlt.sources.rest_api import rest_api_resources
from dlt.sources.rest_api.typing import RESTAPIConfig


# if no argument is provided, `access_token` is read from `.dlt/secrets.toml`
@dlt.source
def open_library_rest_api_source(access_token: str = dlt.secrets.value):
    """Define dlt resources from REST API endpoints."""
    config: RESTAPIConfig = {
        "client": {
            # base URL for Open Library API
            "base_url": "https://openlibrary.org",
            # no authentication is required for the public books endpoint
            # (access_token parameter is not used)
        },
        "resources": [
            # define a resource for the books endpoint using EndpointResource
            {
                "name": "books",
                "endpoint": {
                    "path": "/api/books",
                    "method": "GET",
                    "params": {
                        # retrieve a known ISBN as a simple test
                        "bibkeys": "ISBN:0451526538",
                        "format": "json",
                        "jscmd": "data",
                    },
                    # don't attempt to parse the response with jsonpath;
                    # the endpoint already returns a simple json object
                    "data_selector": "$"
                }
            }
        ],
        # no global defaults needed for now
    }

    yield from rest_api_resources(config)


pipeline = dlt.pipeline(
    pipeline_name='open_library_pipeline',
    destination='duckdb',
    # `refresh="drop_sources"` ensures the data and the state is cleaned
    # on each `pipeline.run()`; remove the argument once you have a
    # working pipeline.
    refresh="drop_sources",
    # show basic progress of resources extracted, normalized files and load-jobs on stdout
    progress="log",
)


if __name__ == "__main__":
    load_info = pipeline.run(open_library_rest_api_source())
    print(load_info)  # noqa: T201
