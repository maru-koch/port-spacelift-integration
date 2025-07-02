import os
import uuid
from loguru import logger
<<<<<<< HEAD
from constants import ResourceType
from port_ocean.context.ocean import ocean
from spacelift.client import SpaceliftClient
from typing import AsyncGenerator, List, Dict, Any
from port_ocean.core.handlers import JQEntityProcessor
from port_ocean.core.integrations.base import BaseIntegration


=======
from port_ocean.context.ocean import ocean
from port_ocean.core.integrations.base import BaseIntegration
from port_ocean.core.handlers import JQEntityProcessor
from spacelift.client import SpaceliftClient


from typing import AsyncGenerator, List, Dict, Any
from constants import ResourceType

>>>>>>> 5681dd7b985679192bdbb4d560b4b79dd309bc6b
class SpaceliftIntegration(BaseIntegration):
    def __init__(self):
        super().__init__()
        self.client = None
        self.entity_processor = JQEntityProcessor()

    def init_client(self) -> SpaceliftClient:
<<<<<<< HEAD
        """Initialize Spacelift client using ocean.integration_config."""
        if not self.client:
            try:
                self.client = SpaceliftClient(
                    api_key=ocean.integration_config["apiKey"],
                    api_secret=ocean.integration_config["apiSecret"],
=======
        """Initialize Spacelift client with correct authentication."""
        if not self.client:
            try:
                self.client = SpaceliftClient(
                    api_token=ocean.integration_config["apiToken"],
>>>>>>> 5681dd7b985679192bdbb4d560b4b79dd309bc6b
                    endpoint=ocean.integration_config["apiEndpoint"]
                )
                logger.info("Spacelift client initialized successfully")
            except Exception as e:
                logger.error(f"Failed to initialize Spacelift client: {e}", exc_info=True)
                raise
        return self.client

    @ocean.on_start()
    async def on_start(self) -> None:
        """Log integration startup."""
        logger.info("Starting Port Ocean Spacelift integration")
<<<<<<< HEAD
        self.init_client()  # Initialize client on start

    @ocean.on_resync(ResourceType.SPACE)
    async def on_resync_spaces(self) -> AsyncGenerator[List[Dict], None]:
=======
        self.init_client()

    @ocean.on_resync(ResourceType.SPACE)
    async def on_resync_spaces(self) -> AsyncGenerator[List[Dict], None]:
        """Resync spaces with pagination."""
>>>>>>> 5681dd7b985679192bdbb4d560b4b79dd309bc6b
        trace_id = str(uuid.uuid4())
        with logger.contextualize(trace_id=trace_id):
            logger.info("Resyncing spaces")
            client = self.init_client()
<<<<<<< HEAD
            data = await client.get_spaces()
            entities = self.entity_processor.map_to_entities(data, "mappings/spaces.json")
            logger.debug(f"Yielding {len(entities)} space entities")
            yield entities

    @ocean.on_resync(ResourceType.STACK)
    async def on_resync_stacks(self) -> AsyncGenerator[List[Dict], None]:
=======
            async for batch in client.get_paginated_spaces():
                entities = self.entity_processor.map_to_entities(batch, "mappings/spaces.json")
                logger.debug(f"Yielding {len(entities)} space entities")
                yield entities

    @ocean.on_resync(ResourceType.STACK)
    async def on_resync_stacks(self) -> AsyncGenerator[List[Dict], None]:
        """Resync stacks with pagination."""
>>>>>>> 5681dd7b985679192bdbb4d560b4b79dd309bc6b
        trace_id = str(uuid.uuid4())
        with logger.contextualize(trace_id=trace_id):
            logger.info("Resyncing stacks")
            client = self.init_client()
<<<<<<< HEAD
            data = await client.get_stacks()
            entities = self.entity_processor.map_to_entities(data, "mappings/stacks.json")
            logger.debug(f"Yielding {len(entities)} stack entities")
            yield entities

    @ocean.on_resync(ResourceType.DEPLOYMENT)
    async def on_resync_deployments(self, filters: Dict = None) -> AsyncGenerator[List[Dict], None]:
=======
            async for batch in client.get_paginated_stacks():
                entities = self.entity_processor.map_to_entities(batch, "mappings/stacks.json")
                logger.debug(f"Yielding {len(entities)} stack entities")
                yield entities

    @ocean.on_resync(ResourceType.DEPLOYMENT)
    async def on_resync_deployments(self, filters: Dict = None) -> AsyncGenerator[List[Dict], None]:
        """Resync deployments with pagination."""
>>>>>>> 5681dd7b985679192bdbb4d560b4b79dd309bc6b
        trace_id = str(uuid.uuid4())
        with logger.contextualize(trace_id=trace_id):
            logger.info("Resyncing deployments")
            client = self.init_client()
            filters = filters or ocean.integration_config.get("filters", {})
<<<<<<< HEAD
            async for batch in client.get_deployments(filters):
=======
            async for batch in client.get_paginated_deployments(filters):
>>>>>>> 5681dd7b985679192bdbb4d560b4b79dd309bc6b
                entities = self.entity_processor.map_to_entities(batch, "mappings/deployments.json")
                logger.debug(f"Yielding {len(entities)} deployment entities")
                yield entities

    @ocean.on_resync(ResourceType.POLICY)
    async def on_resync_policies(self) -> AsyncGenerator[List[Dict], None]:
<<<<<<< HEAD
=======
        """Resync policies with pagination."""
>>>>>>> 5681dd7b985679192bdbb4d560b4b79dd309bc6b
        trace_id = str(uuid.uuid4())
        with logger.contextualize(trace_id=trace_id):
            logger.info("Resyncing policies")
            client = self.init_client()
<<<<<<< HEAD
            data = await client.get_policies()
            entities = self.entity_processor.map_to_entities(data, "mappings/policies.json")
            logger.debug(f"Yielding {len(entities)} policy entities")
            yield entities

    @ocean.on_resync(ResourceType.USER)
    async def on_resync_users(self) -> AsyncGenerator[List[Dict], None]:
=======
            async for batch in client.get_paginated_policies():
                entities = self.entity_processor.map_to_entities(batch, "mappings/policies.json")
                logger.debug(f"Yielding {len(entities)} policy entities")
                yield entities

    @ocean.on_resync(ResourceType.USER)
    async def on_resync_users(self) -> AsyncGenerator[List[Dict], None]:
        """Resync users with pagination."""
>>>>>>> 5681dd7b985679192bdbb4d560b4b79dd309bc6b
        trace_id = str(uuid.uuid4())
        with logger.contextualize(trace_id=trace_id):
            logger.info("Resyncing users")
            client = self.init_client()
<<<<<<< HEAD
            data = await client.get_users()
            entities = self.entity_processor.map_to_entities(data, "mappings/users.json")
            logger.debug(f"Yielding {len(entities)} user entities")
            yield entities

    @ocean.on_resync()
    async def on_resync_generic(self, kind: str) -> AsyncGenerator[List[Dict], None]:
=======
            async for batch in client.get_paginated_users():
                entities = self.entity_processor.map_to_entities(batch, "mappings/users.json")
                logger.debug(f"Yielding {len(entities)} user entities")
                yield entities

    @ocean.on_resync()
    async def on_resync_generic(self, kind: str) -> AsyncGenerator[List[Dict], None]:
        """Resync generic resources."""
>>>>>>> 5681dd7b985679192bdbb4d560b4b79dd309bc6b
        trace_id = str(uuid.uuid4())
        with logger.contextualize(trace_id=trace_id):
            logger.info(f"Resyncing generic kind: {kind}")
            mapping_file = f"mappings/{kind}.json"
            if not os.path.exists(mapping_file):
                logger.error(f"Mapping file for kind {kind} not found")
                return
            try:
                client = self.init_client()
<<<<<<< HEAD
                async for batch in client.get_generic_resource(kind):
=======
                async for batch in client.get_paginated_generic_resource(kind):
>>>>>>> 5681dd7b985679192bdbb4d560b4b79dd309bc6b
                    entities = self.entity_processor.map_to_entities(batch, mapping_file)
                    logger.debug(f"Yielding {len(entities)} {kind} entities")
                    yield entities
            except Exception as e:
                logger.error(f"Error resyncing {kind}: {e}", exc_info=True)

    @ocean.router.post("/webhook")
    async def handle_webhook(self, payload: Dict[Any, Any]) -> None:
<<<<<<< HEAD
=======
        """Handle Spacelift webhook events."""
>>>>>>> 5681dd7b985679192bdbb4d560b4b79dd309bc6b
        trace_id = str(uuid.uuid4())
        with logger.contextualize(trace_id=trace_id):
            event_type = payload.get("event", "").lower()
            resource_id = payload.get("id")
            logger.info(f"Received webhook: event={event_type}, resource_id={resource_id}")
            
            kind_map = {
                "run.created": ResourceType.DEPLOYMENT,
                "run.finished": ResourceType.DEPLOYMENT,
                "run.failed": ResourceType.DEPLOYMENT,
                "stack.created": ResourceType.STACK,
                "policy.created": ResourceType.POLICY
            }
            kind = kind_map.get(event_type)
            if not kind:
                logger.warning(f"Unsupported webhook event: {event_type}")
                return
            
            mapping_file = f"mappings/{kind}.json"
            if not os.path.exists(mapping_file):
                logger.error(f"Mapping file for kind {kind} not found")
                return
            
            logger.info(f"Processing webhook for kind: {kind}")
            client = self.init_client()
            if kind == ResourceType.DEPLOYMENT:
<<<<<<< HEAD
                data = await client.get_deployments({"id": resource_id})
            else:
                data = await client.get_generic_resource(kind)
=======
                data = await client.get_paginated_deployments({"id": resource_id})
                data = data[0] if data else []  # Handle single-item batch
            else:
                data = await client.get_paginated_generic_resource(kind)
                data = data[0] if data else []
>>>>>>> 5681dd7b985679192bdbb4d560b4b79dd309bc6b
            
            entities = self.entity_processor.map_to_entities(data, mapping_file)
            await ocean.port_client.upsert_entities(kind, entities)
            logger.info(f"Processed webhook for {kind}, resource_id={resource_id}", entity_count=len(entities))

if __name__ == "__main__":
<<<<<<< HEAD
    from logging_config import setup_logging
=======
    from log_config import setup_logging
>>>>>>> 5681dd7b985679192bdbb4d560b4b79dd309bc6b
    setup_logging()
    logger.info("Spacelift integration module loaded")