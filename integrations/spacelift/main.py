
import os
import asyncio
from port_ocean.context.ocean import ocean
from port_ocean.core.handlers import EntityProcessor
from port_ocean.core.ocean_types import OCEAN_INTEGRATION_TYPE, ASYNC_GENERATOR_RESYNC_TYPE
from loguru import logger
from spacelift.client import SpaceliftClient
import constants

class SpaceliftIntegration(OCEAN_INTEGRATION_TYPE):
    def __init__(self):
        super().__init__()
        self.client = None
        self.entity_processor = EntityProcessor()

    def init_client(self):
        """Initialize Spacelift client with correct authentication."""
        if not self.client:
            try:
                self.client = SpaceliftClient(
                    api_token=ocean.integration_config["apiToken"],
                    endpoint=ocean.integration_config["apiEndpoint"]
                )
                logger.info("Spacelift client initialized successfully")
            except Exception as e:
                logger.error(f"Failed to initialize Spacelift client: {e}", exc_info=True)
                raise
        return self.client 
        
    @ocean.on_resync("space")
    async def on_resync_spaces(self) -> ASYNC_GENERATOR_RESYNC_TYPE:
        self.log.info("Resyncing spaces")
        data = await self.client.get_spaces()
        yield self.entity_processor.map_to_entities(data, "mappings/spaces.json")

    @ocean.on_resync("stack")
    async def on_resync_stacks(self) -> ASYNC_GENERATOR_RESYNC_TYPE:
        self.log.info("Resyncing stacks")
        data = await self.client.get_stacks()
        yield self.entity_processor.map_to_entities(data, "mappings/stacks.json")

    @ocean.on_resync("deployment")
    async def on_resync_deployments(self, filters: dict = None) -> ASYNC_GENERATOR_RESYNC_TYPE:
        self.log.info("Resyncing deployments")
        filters = filters or self.config.get("filters", {})
        async for batch in self.client.get_deployments(filters):  # Assume get_deployments yields batches
            yield self.entity_processor.map_to_entities(batch, "mappings/deployments.json")

    @ocean.on_resync("policy")
    async def on_resync_policies(self) -> ASYNC_GENERATOR_RESYNC_TYPE:
        self.log.info("Resyncing policies")
        data = await self.client.get_policies()
        yield self.entity_processor.map_to_entities(data, "mappings/policies.json")

    @ocean.on_resync("user")
    async def on_resync_users(self) -> ASYNC_GENERATOR_RESYNC_TYPE:
        self.log.info("Resyncing users")
        data = await self.client.get_users()
        yield self.entity_processor.map_to_entities(data, "mappings/users.json")

    @ocean.on_resync()
    async def on_resync_generic(self, kind: str) -> ASYNC_GENERATOR_RESYNC_TYPE:
        self.log.info(f"Resyncing generic kind: {kind}")
        mapping_file = f"mappings/{kind}.json"
        if not os.path.exists(mapping_file):
            self.log.error(f"Mapping file for kind {kind} not found")
            return
        try:
            async for batch in self.client.get_generic_resource(kind):  # Assume get_generic_resource yields batches
                yield self.entity_processor.map_to_entities(batch, mapping_file)
        except Exception as e:
            self.log.error(f"Error resyncing {kind}: {str(e)}")

    async def handle_webhook(self, payload: dict) -> None:
        event_type = payload.get("event", "").lower()
        resource_id = payload.get("id")
        self.log.info(f"Received webhook: event={event_type}, resource_id={resource_id}")
        
        kind_map = {
            "run.created": "deployment",
            "run.finished": "deployment",
            "run.failed": "deployment",
            "stack.created": "stack",
            "policy.created": "policy"
        }
        kind = kind_map.get(event_type)
        if not kind:
            self.log.warning(f"Unsupported webhook event: {event_type}")
            return
        
        mapping_file = f"mappings/{kind}.json"
        if not os.path.exists(mapping_file):
            self.log.error(f"Mapping file for kind {kind} not found")
            return
        
        if kind == constants.ResourceType.DEPLOYMENT:
            data = await self.client.get_deployments({"id": resource_id})
        else:
            data = await self.client.get_generic_resource(kind)
        
        entities = self.entity_processor.map_to_entities(data, mapping_file)
        await self.port_client.upsert_entities(kind, entities)
        self.log.info(f"Processed webhook for {kind}, resource_id={resource_id}")

async def main():
    integration = SpaceliftIntegration()
    await integration.start()

if __name__ == "__main__":
    asyncio.run(main())