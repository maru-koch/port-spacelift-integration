from loguru import logger
from datetime import datetime, timedelta
from port_ocean.utils import http_async_client
from tenacity import retry, stop_after_attempt, wait_exponential

import queries

class SpaceliftClient:
    def __init__(self, api_token: str, endpoint: str):
        self.endpoint = endpoint
        self.headers = {"Authorization": f"Bearer {api_token}", "Content-Type": "application/json"}

    