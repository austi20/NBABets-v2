from __future__ import annotations

import importlib

import pytest
from fastapi.routing import APIRoute

_ROUTER_MODULES = (
    "app.server.routers.board",
    "app.server.routers.insights",
    "app.server.routers.local_agent",
    "app.server.routers.parlays",
    "app.server.routers.props",
    "app.server.routers.startup",
    "app.server.routers.trading",
)


def _iter_routes():
    for module_name in _ROUTER_MODULES:
        module = importlib.import_module(module_name)
        router = module.router
        for route in router.routes:
            if isinstance(route, APIRoute):
                yield module_name, route


def test_all_routes_have_docstrings() -> None:
    missing = [
        f"{module_name}:{route.endpoint.__name__}"
        for module_name, route in _iter_routes()
        if not (route.endpoint.__doc__ or "").strip()
    ]
    assert not missing, f"routes missing one-line docstrings: {missing}"


@pytest.mark.parametrize("module_name", _ROUTER_MODULES)
def test_router_module_importable(module_name: str) -> None:
    module = importlib.import_module(module_name)
    assert module.router is not None
