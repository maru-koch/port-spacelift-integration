
from port_ocean.context.ocean import ocean
from spacelift.client import SpaceliftClient


def create_spacelift_client() -> SpaceliftClient:
    return SpaceliftClient(
        api_token=ocean.integration_config.get("api_token"),
        endpoint=ocean.integration_config.get("api_endpoint"),
    )