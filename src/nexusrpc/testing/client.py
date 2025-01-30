from dataclasses import dataclass
from typing import Any

import httpx


@dataclass
class ServiceClient:
    server_address: str  # E.g. http://127.0.0.1:7243
    endpoint: str
    service: str

    async def start_operation(
        self,
        operation: str,
        body: dict[str, Any],
        headers: dict[str, str] = {},
    ) -> httpx.Response:
        """
        Start a Nexus operation.
        """
        async with httpx.AsyncClient() as http_client:
            return await http_client.post(
                f"{self.server_address}/nexus/endpoints/{self.endpoint}/services/{self.service}/{operation}",
                json=body,
                headers=headers,
            )

    async def fetch_operation_info(
        self,
        operation: str,
        token: str,
    ) -> httpx.Response:
        async with httpx.AsyncClient() as http_client:
            return await http_client.get(
                f"{self.server_address}/nexus/endpoints/{self.endpoint}/services/{self.service}/{operation}",
                # Token can also be sent as "Nexus-Operation-Token" header
                params={"token": token},
            )

    async def fetch_operation_result(
        self,
        operation: str,
        token: str,
    ) -> httpx.Response:
        async with httpx.AsyncClient() as http_client:
            return await http_client.get(
                f"{self.server_address}/nexus/endpoints/{self.endpoint}/services/{self.service}/{operation}/result",
                # Token can also be sent as "Nexus-Operation-Token" header
                params={"token": token},
            )

    async def cancel_operation(
        self,
        operation: str,
        token: str,
    ) -> httpx.Response:
        async with httpx.AsyncClient() as http_client:
            return await http_client.post(
                f"{self.server_address}/nexus/endpoints/{self.endpoint}/services/{self.service}/{operation}/cancel",
                # Token can also be sent as "Nexus-Operation-Token" header
                params={"token": token},
            )
