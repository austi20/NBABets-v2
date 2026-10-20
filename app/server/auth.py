from __future__ import annotations

import hmac
from collections.abc import Iterable

from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response


class AppTokenMiddleware(BaseHTTPMiddleware):
    def __init__(
        self,
        app,
        *,
        protected_prefixes: Iterable[str],
        exempt_paths: Iterable[str] | None = None,
    ) -> None:
        super().__init__(app)
        self._protected_prefixes = tuple(protected_prefixes)
        self._exempt_paths = set(exempt_paths or ())

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        if request.url.path in self._exempt_paths:
            return await call_next(request)
        if request.method == "OPTIONS" and "access-control-request-method" in request.headers:
            return await call_next(request)
        if not request.url.path.startswith(self._protected_prefixes):
            return await call_next(request)

        expected_token = getattr(request.app.state, "app_token", "")
        if not expected_token:
            return JSONResponse({"detail": "server token not configured"}, status_code=503)

        provided_token = request.headers.get("X-App-Token", "")
        if not hmac.compare_digest(provided_token.encode(), expected_token.encode()):
            return JSONResponse({"detail": "missing or invalid X-App-Token"}, status_code=401)
        return await call_next(request)
