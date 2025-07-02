from loguru import logger
from datetime import datetime, timedelta
from port_ocean.utils import http_async_client
from tenacity import retry, stop_after_attempt, wait_exponential

import queries

class SpaceliftClient:
    def __init__(self, api_token: str, endpoint: str):
        self.endpoint = endpoint
        self.headers = {"Authorization": f"Bearer {api_token}", "Content-Type": "application/json"}

    @property
    def auth_headers(self):
        """Get Authorization headers. """
        return {"Authorization":f"Bearer {self.token}"}
    
    async def _refresh_token(self) -> str:
        self.logger.info("Refreshing Spacelift API token")
        payload = {"key_id": self.api_key, "key_secret": self.api_secret}
        response = await self.client.post(f"{self.endpoint}/auth", json=payload)
        response.raise_for_status()
        data = response.json()
        self.token = data["token"]
        self.token_expiry = datetime.now() + timedelta(seconds=data.get("expires_in", 3600))  # Default 1 hour
        self.logger.info(f"Token refreshed, expires at {self.token_expiry}")
        return self.token
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    async def _graphql_query(self, query: str, variables: dict = None) -> dict:
        if not self.token:
            await self._refresh_token()
        try:
            response = await self.client.post(
                self.endpoint,
                json={"query": query, "variables": variables or {}},
                headers=self.auth_headers
            )
            response.raise_for_status()
            data = response.json()
            
            # Dynamic rate limit handling
            limit = int(response.headers.get("X-RateLimit-Limit", 1000))
            remaining = int(response.headers.get("X-RateLimit-Remaining", 1000))
            self.logger.info(f"Rate limit: {remaining}/{limit} remaining")
            if remaining < max(10, limit * 0.1):  # Trigger retry if <10 or 10% of limit
                self.logger.warning(f"Rate limit low: {remaining}/{limit}")
                raise Exception("Rate limit approaching")

            return data
        except Exception as e:
            if "401" in str(e):
                self.logger.info("Token expired, refreshing")
                await self._refresh_token()
                return await self._graphql_query(query, variables)
            self.logger.error(f"API error: {str(e)}")
            raise

    async def get_spaces(self) -> list[dict]:
        data = await self._graphql_query(queries.SPACES)
        return data["data"]["spaces"]

    async def get_stacks(self) -> list[dict]:
        data = await self._graphql_query(queries.STACKS)
        return data["data"]["stacks"]

    async def get_deployments(self, filters: dict) -> list[dict]:
        status_filter = filters.get("deployment_status", [])
        time_filter = filters.get("last_n_days", 7)
        variables = {"status": status_filter} if status_filter else {}
        results = []
        has_next = True
        cursor = None

        while has_next:
            variables["after"] = cursor
            data = await self._graphql_query(queries.DEPLOYMENTS, variables)
            runs = data["data"]["runs"]["edges"]
            results.extend([run["node"] for run in runs])
            has_next = data["data"]["runs"]["pageInfo"]["hasNextPage"]
            cursor = data["data"]["runs"]["pageInfo"]["endCursor"]

        return results

    async def get_policies(self) -> list[dict]:
        data = await self._graphql_query(queries.POLICIES)
        return data["data"]["policies"]

    async def get_users(self) -> list[dict]:
        data = await self._graphql_query(queries.USERS)
        return data["data"]["users"]

    async def get_generic_resource(self, kind: str) -> list[dict]:
        """Generic method for fetching any Spacelift resource."""
        query = f"""
        query {{
          {kind} {{
            id
            name
          }}
        }}
        """
        data = await self._graphql_query(query)
        return data["data"][kind]
    
