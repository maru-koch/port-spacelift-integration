import pytest
import pytest_asyncio
from unittest.mock import AsyncMock, patch
from spacelift.client import SpaceliftClient
from main import SpaceliftIntegration

@pytest_asyncio.asyncio
async def test_get_spaces():
    client = SpaceliftClient("key", "secret", "https://test.app.spacelift.io/graphql")
    with patch.object(client.client, "post", new=AsyncMock()) as mock_post:
        mock_post.return_value.json.return_value = {
            "data": {"spaces": [{"id": "space1", "name": "Test Space", "description": "Test"}]}
        }
        spaces = await client.get_spaces()
        assert len(spaces) == 1
        assert spaces[0]["id"] == "space1"

@pytest_asyncio.asyncio
async def test_get_deployments_filtered():
    client = SpaceliftClient("key", "secret", "https://test.app.spacelift.io/graphql")
    with patch.object(client.client, "post", new=AsyncMock()) as mock_post:
        mock_post.return_value.json.return_value = {
            "data": {
                "runs": {
                    "edges": [{"node": {"id": "run1", "state": "finished", "createdAt": "2025-06-18T00:00:00Z", "commit": {"hash": "abc123", "message": "test"}, "triggeredBy": "user1", "stackId": "stack1"}}],
                    "pageInfo": {"hasNextPage": False, "endCursor": None}
                }
            }
        }
        deployments = await client.get_deployments({"deployment_status": ["finished"], "last_n_days": 7})
        assert len(deployments) == 1
        assert deployments[0]["id"] == "run1"

@pytest_asyncio.asyncio
async def test_handle_webhook():
    integration = SpaceliftIntegration()
    with patch.object(integration.client, "get_deployments", new=AsyncMock()) as mock_get, \
         patch.object(integration.port_client, "upsert_entities", new=AsyncMock()) as mock_upsert:
        mock_get.return_value = [{"id": "run1", "state": "finished", "createdAt": "2025-06-18T00:00:00Z", "commit": {"hash": "abc123", "message": "test"}, "triggeredBy": "user1", "stackId": "stack1"}]
        await integration.handle_webhook({"event": "run.finished", "id": "run1"})
        mock_get.assert_called_once()
        mock_upsert.assert_called_once()