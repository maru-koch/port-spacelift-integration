
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
        
    @ocean.on_start()
    async def on_start(self) -> None:
        """Log integration startup."""
        logger.info("Starting Port Ocean Spacelift integration")
        self.init_client()

    