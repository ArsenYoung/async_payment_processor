import asyncio
import ipaddress
import socket
from collections.abc import AsyncIterator, Iterable, Iterator, Sequence
from contextlib import contextmanager

import httpcore
import httpx


class WebhookDestinationError(ValueError):
    pass


class RestrictedBackend(httpcore.AsyncNetworkBackend):
    def __init__(self, allowed_private_hosts: Sequence[str] = ()) -> None:
        self._allowed_private_hosts = {
            host.lower().rstrip(".") for host in allowed_private_hosts
        }
        self._backend = httpcore.AnyIOBackend()

    async def connect_tcp(
        self,
        host: str,
        port: int,
        timeout: float | None = None,
        local_address: str | None = None,
        socket_options: Iterable[httpcore.SOCKET_OPTION] | None = None,
    ) -> httpcore.AsyncNetworkStream:
        try:
            async with asyncio.timeout(timeout):
                addresses = await asyncio.get_running_loop().getaddrinfo(
                    host, port, type=socket.SOCK_STREAM
                )
                candidates = list(dict.fromkeys(str(item[4][0]) for item in addresses))
                if not candidates:
                    raise httpcore.ConnectError("Webhook destination has no addresses")
                for candidate in candidates:
                    address = ipaddress.ip_address(candidate)
                    if (
                        isinstance(address, ipaddress.IPv6Address)
                        and address.ipv4_mapped is not None
                    ):
                        address = address.ipv4_mapped
                    transition_address = isinstance(
                        address, ipaddress.IPv6Address
                    ) and (
                        address.sixtofour is not None
                        or address.teredo is not None
                        or address in ipaddress.IPv6Network("64:ff9b::/96")
                    )
                    if (
                        transition_address
                        or not address.is_global
                        or address.is_multicast
                        or address.is_reserved
                        or getattr(address, "is_site_local", False)
                        or getattr(address, "scope_id", None) is not None
                    ):
                        if host.lower().rstrip(".") not in self._allowed_private_hosts:
                            raise WebhookDestinationError(
                                "Webhook destination is not permitted"
                            )
                last_error = None
                for candidate in candidates:
                    try:
                        return await self._backend.connect_tcp(
                            candidate,
                            port,
                            timeout=timeout,
                            local_address=local_address,
                            socket_options=socket_options,
                        )
                    except (httpcore.ConnectError, httpcore.ConnectTimeout) as error:
                        last_error = error
                assert last_error is not None
                raise last_error
        except TimeoutError as error:
            raise httpcore.ConnectTimeout("Webhook connection timed out") from error
        except OSError as error:
            raise httpcore.ConnectError("Webhook DNS lookup failed") from error

    async def connect_unix_socket(
        self,
        path: str,
        timeout: float | None = None,
        socket_options: Iterable[httpcore.SOCKET_OPTION] | None = None,
    ) -> httpcore.AsyncNetworkStream:
        raise WebhookDestinationError("Unix sockets are not permitted")

    async def sleep(self, seconds: float) -> None:
        await asyncio.sleep(seconds)


@contextmanager
def map_transport_errors() -> Iterator[None]:
    try:
        yield
    except (
        httpcore.TimeoutException,
        httpcore.NetworkError,
        httpcore.ProtocolError,
        httpcore.ProxyError,
        httpcore.UnsupportedProtocol,
    ) as error:
        error_type = getattr(httpx, type(error).__name__, httpx.TransportError)
        raise error_type(str(error)) from error


class ResponseStream(httpx.AsyncByteStream):
    def __init__(self, response: httpcore.Response) -> None:
        self._response = response

    async def __aiter__(self) -> AsyncIterator[bytes]:
        with map_transport_errors():
            async for chunk in self._response.aiter_stream():
                yield chunk

    async def aclose(self) -> None:
        with map_transport_errors():
            await self._response.aclose()


class RestrictedWebhookTransport(httpx.AsyncBaseTransport):
    def __init__(self, allowed_private_hosts: Sequence[str] = ()) -> None:
        self._pool = httpcore.AsyncConnectionPool(
            network_backend=RestrictedBackend(allowed_private_hosts),
            retries=0,
        )

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        if (
            request.url.scheme not in {"http", "https"}
            or request.url.username
            or request.url.password
        ):
            raise WebhookDestinationError(
                "Webhook URL must use HTTP(S) without credentials"
            )
        with map_transport_errors():
            response = await self._pool.handle_async_request(
                httpcore.Request(
                    method=request.method,
                    url=httpcore.URL(
                        scheme=request.url.raw_scheme,
                        host=request.url.raw_host,
                        port=request.url.port,
                        target=request.url.raw_path,
                    ),
                    headers=request.headers.raw,
                    content=request.stream,
                    extensions=request.extensions,
                )
            )
        return httpx.Response(
            response.status,
            headers=response.headers,
            stream=ResponseStream(response),
            extensions=response.extensions,
        )

    async def aclose(self) -> None:
        await self._pool.aclose()
