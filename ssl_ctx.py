"""Shared SSL context using certifi's CA bundle.

The python.org macOS installer ships without a system CA store, so default
TLS verification fails out of the box. Using certifi sidesteps that for
every WebSocket / HTTP connection in the project.
"""
from __future__ import annotations

import ssl

import certifi

_CTX: ssl.SSLContext | None = None


def get_ssl_context() -> ssl.SSLContext:
    global _CTX
    if _CTX is None:
        _CTX = ssl.create_default_context(cafile=certifi.where())
    return _CTX
