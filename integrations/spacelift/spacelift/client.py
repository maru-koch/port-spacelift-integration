from loguru import logger
from aiolimiter import AsyncLimiter
from httpx import Timeout, HTTPStatusError
from port_ocean.utils import http_async_client
from typing import AsyncGenerator, List, Dict, Any, Optional

PAGE_LIMIT = 100

class SpaceliftClient:
    def __init__(self, api_token: str, endpoint: str):
        self.spacelift_api_token = api_token
        self.spacelift_endpoint = endpoint.rstrip("/")  # Ensure no trailing slash
        self.http_client = http_async_client
        self.http_client.timeout = Timeout(30)
        self.rate_limiter = AsyncLimiter(100, 30)

    @property
    def headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self.spacelift_api_token}",
            "Content-Type": "application/json",
        }

    async def _send_request(
        self,
        method: str = "GET",
        url: str | None = None,
        params: Optional[Dict[str, Any]] = None,
        json: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Send an HTTP request with rate limiting and error handling."""
        async with self.rate_limiter:
            try:
                response = await self.http_client.request(
                    method=method,
                    url=url or self.spacelift_endpoint,
                    headers=self.headers,
                    params=params,
                    json=json,
                )
                response.raise_for_status()
                return response.json()
            except HTTPStatusError as e:
                logger.error(
                    f"HTTP error on {method} {url or self.spacelift_endpoint} "
                    f"with params: {params}, status: {e.response.status_code}, "
                    f"response: {e.response.text}"
                )
                raise
            except Exception as e:
                logger.error(
                    f"Unexpected error on {method} {url or self.spacelift_endpoint} "
                    f"with params: {params}, error: {str(e)}"
                )
                raise

    async def _query(
        self, query: str, variables: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Execute a GraphQL query."""
        payload = {"query": query, "variables": variables or {}}
        return await self._send_request(
            method="POST", json=payload
        )

    async def _fetch_paginated(
        self, query: str, resource_key: str, variables: Optional[Dict[str, Any]] = None
    ) -> AsyncGenerator[List[Dict], None]:
        """Generic pagination handler for Spacelift resources."""
        cursor = None
        while True:
            current_variables = {**variables, "after": cursor} if variables else {"after": cursor}
            data = await self._query(query, current_variables)
            edges = data.get("data", {}).get(resource_key, {}).get("edges", [])
            resources = [edge["node"] for edge in edges]
            if resources:
                yield resources
            page_info = data.get("data", {}).get(resource_key, {}).get("pageInfo", {})
            if not page_info.get("hasNextPage", False):
                break
            cursor = page_info.get("endCursor")

    async def get_paginated_spaces(self) -> AsyncGenerator[List[Dict], None]:
        """Fetch spaces with pagination."""
        query = """
        query Spaces($after: String) {
            spaces(first: 100, after: $after) {
                edges { node { id name description createdAt } }
                pageInfo { endCursor hasNextPage }
            }
        }
        """
        async for spaces in self._fetch_paginated(query, "spaces"):
            yield spaces

    async def get_paginated_stacks(self) -> AsyncGenerator[List[Dict], None]:
        """Fetch stacks with pagination."""
        query = """
        query Stacks($after: String) {
            stacks(first: 100, after: $after) {
                edges { node { id name description state createdAt spaceId } }
                pageInfo { endCursor hasNextPage }
            }
        }
        """
        async for stacks in self._fetch_paginated(query, "stacks"):
            yield stacks

    async def get_paginated_deployments(self, filters: Optional[Dict] = None) -> AsyncGenerator[List[Dict], None]:
        """Fetch deployments with pagination."""
        query = """
        query Runs($after: String, $id: ID) {
            runs(first: 100, after: $after, id: $id) {
                edges { node { id stackId state createdAt } }
                pageInfo { endCursor hasNextPage }
            }
        }
        """
        variables = {"id": filters.get("id")} if filters and "id" in filters else None
        async for runs in self._fetch_paginated(query, "runs", variables):
            yield runs

    async def get_paginated_policies(self) -> AsyncGenerator[List[Dict], None]:
        """Fetch policies with pagination."""
        query = """
        query Policies($after: String) {
            policies(first: 100, after: $after) {
                edges { node { id name description type createdAt } }
                pageInfo { endCursor hasNextPage }
            }
        }
        """
        async for policies in self._fetch_paginated(query, "policies"):
            yield policies

    async def get_paginated_users(self) -> AsyncGenerator[List[Dict], None]:
        """Fetch users with pagination."""
        query = """
        query Users($after: String) {
            users(first: 100, after: $after) {
                edges { node { id name email createdAt } }
                pageInfo { endCursor hasNextPage }
            }
        }
        """
        async for users in self._fetch_paginated(query, "users"):
            yield users

    async def get_paginated_generic_resource(self, kind: str) -> AsyncGenerator[List[Dict], None]:
        """Placeholder for generic resources."""
        logger.warning(f"Generic resource fetching not implemented for kind: {kind}")
        yield []