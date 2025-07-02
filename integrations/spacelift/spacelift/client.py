from port_ocean.utils import http_async_client
from typing import AsyncGenerator, List, Dict, Any
from loguru import logger

class SpaceliftClient:
    def __init__(self, api_token: str, endpoint: str):
        self.endpoint = endpoint
        self.headers = {"Authorization": f"Bearer {api_token}", "Content-Type": "application/json"}

    async def _query(self, query: str, variables: Dict = None) -> Dict:
        """Execute GraphQL query with port_ocean.utils.http_async_client."""
        try:
            response = await http_async_client.post(
                self.endpoint,
                headers=self.headers,
                json={"query": query, "variables": variables or {}}
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Failed to query Spacelift API: {e}", exc_info=True)
            raise

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
        cursor = None
        while True:
            data = await self._query(query, {"after": cursor})
            spaces = [edge["node"] for edge in data.get("data", {}).get("spaces", {}).get("edges", [])]
            if spaces:
                yield spaces
            page_info = data.get("data", {}).get("spaces", {}).get("pageInfo", {})
            if not page_info.get("hasNextPage", False):
                break
            cursor = page_info.get("endCursor")

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
        cursor = None
        while True:
            data = await self._query(query, {"after": cursor})
            stacks = [edge["node"] for edge in data.get("data", {}).get("stacks", {}).get("edges", [])]
            if stacks:
                yield stacks
            page_info = data.get("data", {}).get("stacks", {}).get("pageInfo", {})
            if not page_info.get("hasNextPage", False):
                break
            cursor = page_info.get("endCursor")

    async def get_paginated_deployments(self, filters: Dict = None) -> AsyncGenerator[List[Dict], None]:
        """Fetch deployments with pagination."""
        query = """
        query Runs($after: String, $id: ID) {
            runs(first: 100, after: $after, id: $id) {
                edges { node { id stackId state createdAt } }
                pageInfo { endCursor hasNextPage }
            }
        }
        """
        cursor = None
        variables = {"id": filters.get("id")} if filters else {}
        while True:
            data = await self._query(query, {**variables, "after": cursor})
            runs = [edge["node"] for edge in data.get("data", {}).get("runs", {}).get("edges", [])]
            if runs:
                yield runs
            page_info = data.get("data", {}).get("runs", {}).get("pageInfo", {})
            if not page_info.get("hasNextPage", False):
                break
            cursor = page_info.get("endCursor")

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
        cursor = None
        while True:
            data = await self._query(query, {"after": cursor})
            policies = [edge["node"] for edge in data.get("data", {}).get("policies", {}).get("edges", [])]
            if policies:
                yield policies
            page_info = data.get("data", {}).get("policies", {}).get("pageInfo", {})
            if not page_info.get("hasNextPage", False):
                break
            cursor = page_info.get("endCursor")

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
        cursor = None
        while True:
            data = await self._query(query, {"after": cursor})
            users = [edge["node"] for edge in data.get("data", {}).get("users", {}).get("edges", [])]
            if users:
                yield users
            page_info = data.get("data", {}).get("users", {}).get("pageInfo", {})
            if not page_info.get("hasNextPage", False):
                break
            cursor = page_info.get("endCursor")

    async def get_paginated_generic_resource(self, kind: str) -> AsyncGenerator[List[Dict], None]:
        """Placeholder for generic resources."""
        logger.warning(f"Generic resource fetching not implemented for kind: {kind}")
        yield []