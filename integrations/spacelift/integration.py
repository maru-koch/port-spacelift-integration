from typing import Literal
from port_ocean.core.integrations.base import BaseIntegration
from port_ocean.core.handlers.port_app_config.api import APIPortAppConfig
from port_ocean.core.handlers import APIPortAppConfig
from port_ocean.core.handlers.port_app_config.models import (
    ResourceConfig,
    PortAppConfig,
    Selector,
)


class SpaceConfig(ResourceConfig):
    kind: Literal["space"]   # The resource type

class StackConfig(ResourceConfig):
    kind: Literal["stack"]   # The resource type

class UserConfig(ResourceConfig):
    kind: Literal["user"]   # The resource type

class PolicyConfig(ResourceConfig):
    kind: Literal["policy"]   # The resource type

class DeploymentConfig(ResourceConfig):
    kind: Literal["deployment"]   # The resource type

class SpaceliftIntegrationPortAppConfig(PortAppConfig):
    resources: list[
        StackConfig,
        SpaceConfig,
        UserConfig,
        PolicyConfig,
        DeploymentConfig
    ]

class SpaceLiftIntegration(BaseIntegration):
    class AppConfigHandlerClass(APIPortAppConfig):
        CONFIG_CLASS = SpaceliftIntegrationPortAppConfig