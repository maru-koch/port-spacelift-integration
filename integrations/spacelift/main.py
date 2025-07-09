import os
import uuid
from loguru import logger
from port_ocean.context.ocean import ocean
from port_ocean.core.integrations.base import BaseIntegration
from port_ocean.core.handlers import JQEntityProcessor
from spacelift.client import SpaceliftClient


from typing import AsyncGenerator, List, Dict, Any
from utils import ObjectKind

from initialize import create_spacelift_client

@ocean.on_resync(ObjectKind.SPACE)
async def on_resync_spaces(self) -> AsyncGenerator[List[Dict], None]:
    """Resync spaces with pagination."""
    trace_id = str(uuid.uuid4())
    with logger.contextualize(trace_id=trace_id):
        logger.info("Resyncing spaces")
        client = create_spacelift_client()
        async for spaces in client.get_paginated_spaces():
            yield spaces

@ocean.on_resync(ObjectKind.STACK)
async def on_resync_stacks(self) -> AsyncGenerator[List[Dict], None]:
    """Resync stacks with pagination."""
    trace_id = str(uuid.uuid4())
    with logger.contextualize(trace_id=trace_id):
        logger.info("Resyncing stacks")
        client = create_spacelift_client()
        async for stacks in client.get_paginated_stacks():
            yield stacks

@ocean.on_resync(ObjectKind.DEPLOYMENT)
async def on_resync_deployments(self, filters: Dict = None) -> AsyncGenerator[List[Dict], None]:
    """Resync deployments with pagination."""
    trace_id = str(uuid.uuid4())
    with logger.contextualize(trace_id=trace_id):
        logger.info("Resyncing deployments")
        client = create_spacelift_client()
        filters = filters or ocean.integration_config.get("filters", {})
        async for deployments in client.get_paginated_deployments(filters):
            yield deployments

@ocean.on_resync(ObjectKind.POLICY)
async def on_resync_policies(self) -> AsyncGenerator[List[Dict], None]:
    """Resync policies with pagination."""
    trace_id = str(uuid.uuid4())
    with logger.contextualize(trace_id=trace_id):
        logger.info("Resyncing policies")
        client = create_spacelift_client()
        async for policies in client.get_paginated_policies():
            yield policies

@ocean.on_resync(ObjectKind.USER)
async def on_resync_users(self) -> AsyncGenerator[List[Dict], None]:
    """Resync users with pagination."""
    trace_id = str(uuid.uuid4())
    with logger.contextualize(trace_id=trace_id):
        logger.info("Resyncing users")
        client = create_spacelift_client()
        async for users in client.get_paginated_users():
            yield users

@ocean.on_resync()
async def on_resync_generic(self, kind: str) -> AsyncGenerator[List[Dict], None]:
    """Resync generic resources."""
    trace_id = str(uuid.uuid4())
    with logger.contextualize(trace_id=trace_id):
        logger.info(f"Resyncing generic kind: {kind}")
        try:
            client = create_spacelift_client()
            async for resources in client.get_paginated_generic_resource(kind):
                yield resources
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
            "run.created": ObjectKind.DEPLOYMENT,
            "run.finished": ObjectKind.DEPLOYMENT,
            "run.failed": ObjectKind.DEPLOYMENT,
            "stack.created": ObjectKind.STACK,
            "policy.created": ObjectKind.POLICY
        }
        kind = kind_map.get(event_type)
        if not kind:
            logger.warning(f"Unsupported webhook event: {event_type}")
            return
        
        logger.info(f"Processing webhook for kind: {kind}")
        client = create_spacelift_client()
        if kind == ObjectKind.DEPLOYMENT:
            data = await client.get_paginated_deployments({"id": resource_id})
            data = data[0] if data else []  # Handle single-item batch
        else:
            data = await client.get_paginated_generic_resource(kind)
            data = data[0] if data else []
        
        logger.info(f"Processed webhook for {kind}, resource_id={resource_id}")

@ocean.on_start()
async def on_start() -> None:
    """Log integration startup."""
    logger.info("Starting Port Ocean Spacelift integration")
    print("Spacelift integration is starting...")
    # create_spacelift_client()