import os
import uuid
from loguru import logger
from port_ocean.context.ocean import ocean
from port_ocean.core.integrations.base import BaseIntegration
from port_ocean.core.handlers import JQEntityProcessor
from spacelift.client import SpaceliftClient


from typing import AsyncGenerator, List, Dict, Any
from constants import ResourceType


from initialize import create_spacelift_client

@ocean.on_start()
async def on_start(self) -> None:
    """Log integration startup."""
    logger.info("Starting Port Ocean Spacelift integration")
    self.init_client()

@ocean.on_resync(ResourceType.SPACE)
async def on_resync_spaces(self) -> AsyncGenerator[List[Dict], None]:
    """Resync spaces with pagination."""
    trace_id = str(uuid.uuid4())
    with logger.contextualize(trace_id=trace_id):
        logger.info("Resyncing spaces")
        client = create_spacelift_client()
        async for batch in client.get_paginated_spaces():
            entities = self.entity_processor.map_to_entities(batch, "mappings/spaces.json")
            logger.debug(f"Yielding {len(entities)} space entities")
            yield entities

@ocean.on_resync(ResourceType.STACK)
async def on_resync_stacks(self) -> AsyncGenerator[List[Dict], None]:
    """Resync stacks with pagination."""
    trace_id = str(uuid.uuid4())
    with logger.contextualize(trace_id=trace_id):
        logger.info("Resyncing stacks")
        client = create_spacelift_client()
        async for batch in client.get_paginated_stacks():
            entities = self.entity_processor.map_to_entities(batch, "mappings/stacks.json")
            logger.debug(f"Yielding {len(entities)} stack entities")
            yield entities

@ocean.on_resync(ResourceType.DEPLOYMENT)
async def on_resync_deployments(self, filters: Dict = None) -> AsyncGenerator[List[Dict], None]:
    """Resync deployments with pagination."""
    trace_id = str(uuid.uuid4())
    with logger.contextualize(trace_id=trace_id):
        logger.info("Resyncing deployments")
        client = create_spacelift_client()
        filters = filters or ocean.integration_config.get("filters", {})
        async for batch in client.get_paginated_deployments(filters):
            entities = self.entity_processor.map_to_entities(batch, "mappings/deployments.json")
            logger.debug(f"Yielding {len(entities)} deployment entities")
            yield entities

@ocean.on_resync(ResourceType.POLICY)
async def on_resync_policies(self) -> AsyncGenerator[List[Dict], None]:
    """Resync policies with pagination."""
    trace_id = str(uuid.uuid4())
    with logger.contextualize(trace_id=trace_id):
        logger.info("Resyncing policies")
        client = create_spacelift_client()
        async for batch in client.get_paginated_policies():
            entities = self.entity_processor.map_to_entities(batch, "mappings/policies.json")
            logger.debug(f"Yielding {len(entities)} policy entities")
            yield entities

@ocean.on_resync(ResourceType.USER)
async def on_resync_users(self) -> AsyncGenerator[List[Dict], None]:
    """Resync users with pagination."""
    trace_id = str(uuid.uuid4())
    with logger.contextualize(trace_id=trace_id):
        logger.info("Resyncing users")
        client = create_spacelift_client()
        async for batch in client.get_paginated_users():
            entities = self.entity_processor.map_to_entities(batch, "mappings/users.json")
            logger.debug(f"Yielding {len(entities)} user entities")
            yield entities

@ocean.on_resync()
async def on_resync_generic(self, kind: str) -> AsyncGenerator[List[Dict], None]:
    """Resync generic resources."""
    trace_id = str(uuid.uuid4())
    with logger.contextualize(trace_id=trace_id):
        logger.info(f"Resyncing generic kind: {kind}")
        mapping_file = f"mappings/{kind}.json"
        if not os.path.exists(mapping_file):
            logger.error(f"Mapping file for kind {kind} not found")
            return
        try:
            client = create_spacelift_client()
            async for batch in client.get_paginated_generic_resource(kind):
                entities = self.entity_processor.map_to_entities(batch, mapping_file)
                logger.debug(f"Yielding {len(entities)} {kind} entities")
                yield entities
        except Exception as e:
            logger.error(f"Error resyncing {kind}: {e}", exc_info=True)

@ocean.router.post("/webhook")
async def handle_webhook(self, payload: Dict[Any, Any]) -> None:
    """Handle Spacelift webhook events."""
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
        client = create_spacelift_client()
        if kind == ResourceType.DEPLOYMENT:
            data = await client.get_paginated_deployments({"id": resource_id})
            data = data[0] if data else []  # Handle single-item batch
        else:
            data = await client.get_paginated_generic_resource(kind)
            data = data[0] if data else []
        
        entities = self.entity_processor.map_to_entities(data, mapping_file)
        await ocean.port_client.upsert_entities(kind, entities)
        logger.info(f"Processed webhook for {kind}, resource_id={resource_id}", entity_count=len(entities))

