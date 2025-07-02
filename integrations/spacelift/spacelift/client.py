from loguru import logger
from datetime import datetime, timedelta
from port_ocean.utils import http_async_client
from tenacity import retry, stop_after_attempt, wait_exponential
from typing import AsyncGenerator, List, Dict, Any
from loguru import logger

import queries

class SpaceliftClient:
    def __init__(self, api_token: str, endpoint: str):
        self.endpoint = endpoint
        self.headers = {"Authorization": f"Bearer {api_token}", "Content-Type": "application/json"}



class SpaceliftClient:
    def __init__(self, api_token: str, endpoint: str):
        self.endpoint = endpoint
        self.headers = {"Authorization": f"Bearer {api_token}", "Content-Type": "application/json"}

    async def _query(self, query: str, variables: Dict = None) -> Dict:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                self.endpoint,
                headers=self.headers,
                json={"query": query, "variables": variables or {}}
            ) as resp:
                if resp.status != 200:
                    logger.error(f"Spacelift API error: {await resp.text()}")
                    raise Exception(f"API request failed: {resp.status}")
                return await resp.json()

    async def get_paginated_spaces(self) -> AsyncGenerator[List[Dict], None]:
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
            spaces = [edge["node"] for edge in data["data"]["spaces"]["edges"]]
            yield spaces
            page_info = data["data"]["spaces"]["pageInfo"]
            if not page_info["hasNextPage"]:
                break
            cursor = page_info["endCursor"]

    async def get_paginated_stacks(self) -> AsyncGenerator[List[Dict], None]:
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
            stacks = [edge["node"] for edge in data["data"]["stacks"]["edges"]]
            yield stacks
            page_info = data["data"]["stacks"]["pageInfo"]
            if not page_info["hasNextPage"]:
                break
            cursor = page_info["endCursor"]

    async def get_paginated_deployments(self, filters: Dict = None) -> AsyncGenerator[List[Dict], None]:
        query = """
        query Runs($after: String, $id: String) {
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
            runs = [edge["node"] for edge in data["data"]["runs"]["edges"]]
            yield runs
            page_info = data["data"]["runs"]["pageInfo"]
            if not page_info["hasNextPage"]:
                break
            cursor = page_info["endCursor"]

    async def get_paginated_policies(self) -> AsyncGenerator[List[Dict], None]:
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
            policies = [edge["node"] for edge in data["data"]["policies"]["edges"]]
            yield policies
            page_info = data["data"]["policies"]["pageInfo"]
            if not page_info["hasNextPage"]:
                break
            cursor = page_info["endCursor"]

    async def get_paginated_users(self) -> AsyncGenerator[List[Dict], None]:
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
            users = [edge["node"] for edge in data["data"]["users"]["edges"]]
            yield users
            page_info = data["data"]["users"]["pageInfo"]
            if not page_info["hasNextPage"]:
                break
            cursor = page_info["endCursor"]

    async def get_paginated_generic_resource(self, kind: str) -> AsyncGenerator[List[Dict], None]:
        # Placeholder for generic resources
        yield []