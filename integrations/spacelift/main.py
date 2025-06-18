from port_ocean.core.ocean_types import OCEAN_INTEGRATION_TYPE
from port_ocean.utils import http_async_client
from spacelift.client import SpaceliftClient
from port_ocean.core.handlers import EntityProcessor
from port_ocean.core.models import Entity
from constants import ResourceType
import asyncio

class SpaceliftIntegration(OCEAN_INTEGRATION_TYPE):
    def __init__(self):
        super().__init__()
        self.client = SpaceliftClient(
            self.config["api_key"],
            self.config["api_secret"],
            self.config["api_endpoint"]
        )
        self.entity_processor = EntityProcessor()

    async def _fetch_resources(self, kind: str, filters: dict) -> list[Entity]:
        """Fetch Spacelift resources and map to Port Entities.
        :kind: type of resources
        :filter: status to filter the resource by"""

        if kind == ResourceType.SPACE:
            data = await self.client.get_spaces()
        elif kind == ResourceType.STACK:
            data = await self.client.get_stacks()
        elif kind == ResourceType.DEPLOYMENT:
            data = await self.client.get_deployments(filters)
        elif kind == ResourceType.POLICY:
            data = await self.client.get_policies()
        elif kind == ResourceType.USER:
            data = await self.client.get_users()
        else:
            data = await self.client.get_generic_resource(kind)
        
        return self.entity_processor.map_to_entities(data, f"mappings/{kind}.json")

    async def resync(self, kind: str, filters: dict = None) -> None:
        """Handle full resync for a given kind."""
        self.log.info(f"Starting resync for {kind}")
        entities = await self._fetch_resources(kind, filters or self.config.get("filters", {}))
        await self.port_client.upsert_entities(kind, entities)
        self.log.info(f"Completed resync for {kind}")

    async def handle_webhook(self, payload: dict) -> None:
        """Process Spacelift webhook events."""
        kind = payload.get("type")
        if not kind:
            self.log.error("Webhook payload missing type")
            return
        
        entities = await self._fetch_resources(kind, payload.get("filters", {}))
        await self.port_client.upsert_entities(kind, entities)
        self.log.info(f"Processed webhook for {kind}")

async def main():
    integration = SpaceliftIntegration()
    await integration.start()

if __name__ == "__main__":
    asyncio.run(main())