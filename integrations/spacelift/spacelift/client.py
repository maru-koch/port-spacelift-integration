from port_ocean.utils import http_async_client
from tenacity import retry, stop_after_attempt, wait_exponential
from loguru import logger

class SpaceliftClient:
    def __init__(self, api_key: str, api_secret: str, endpoint: str):
        self.api_key = api_key
        self.api_secret = api_secret
        self.endpoint = endpoint
        self.client = http_async_client
        self.logger = logger
        self.token = None

    async def _refresh_token(self) -> str:
        """Refresh Spacelift API token on expiration."""
        self.logger.info("Refreshing Spacelift API token")
        payload = {"key_id": self.api_key, "key_secret": self.api_secret}
        response = await self.client.post(f"{self.endpoint}/auth", json=payload)
        response.raise_for_status()
        self.token = response.json()["token"]
        return self.token

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    async def _graphql_query(self, query: str, variables: dict = None) -> dict:
        """Execute GraphQL query with rate limit handling."""
        if not self.token:
            await self._refresh_token()

        headers = {"Authorization": f"Bearer {self.token}"}
        try:
            response = await self.client.post(
                self.endpoint,
                json={"query": query, "variables": variables or {}},
                headers=headers
            )
            response.raise_for_status()
            data = response.json()
            
            # Check rate limit
            remaining = int(response.headers.get("X-RateLimit-Remaining", 1000))
            if remaining < 10:
                self.logger.warning("Rate limit low, slowing down")
                raise Exception("Rate limit approaching")
            
            return data
        except Exception as e:
            if "401" in str(e):
                await self._refresh_token()
                return await self._graphql_query(query, variables)
            self.logger.error(f"API error: {str(e)}")
            raise

    async def get_spaces(self) -> list[dict]:
        query = """
        query {
          spaces {
            id
            name
            description
          }
        }
        """
        data = await self._graphql_query(query)
        return data["data"]["spaces"]

    async def get_stacks(self) -> list[dict]:
        query = """
        query {
          stacks {
            id
            name
            state
            branch
          }
        }
        """
        data = await self._graphql_query(query)
        return data["data"]["stacks"]

    async def get_deployments(self, filters: dict) -> list[dict]:
        status_filter = filters.get("deployment_status", [])
        time_filter = filters.get("last_n_days", 7)
        query = """
        query ($status: [RunState!], $after: String) {
          runs(states: $status, first: 100, after: $after) {
            edges {
              node {
                id
                state
                createdAt
              }
            }
            pageInfo {
              endCursor
              hasNextPage
            }
          }
        }
        """
        variables = {"status": status_filter} if status_filter else {}
        results = []
        has_next = True
        cursor = None

        while has_next:
            variables["after"] = cursor
            data = await self._graphql_query(query, variables)
            runs = data["data"]["runs"]["edges"]
            results.extend([run["node"] for run in runs])
            has_next = data["data"]["runs"]["pageInfo"]["hasNextPage"]
            cursor = data["data"]["runs"]["pageInfo"]["endCursor"]

        return results

    async def get_policies(self) -> list[dict]:
        query = """
        query {
          policies {
            id
            name
            type
          }
        }
        """
        data = await self._graphql_query(query)
        return data["data"]["policies"]

    async def get_users(self) -> list[dict]:
        query = """
        query {
          users {
            id
            username
            email
          }
        }
        """
        data = await self._graphql_query(query)
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