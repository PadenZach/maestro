"""Application-side Python SDK configuration for maestro connections."""

import os
from collections.abc import Mapping
from typing import Any
from urllib.parse import urlsplit

_URL_ERROR = "DBOS_CONDUCTOR_URL must be a nonempty wss:// base URL with a hostname"


def _conductor_url(environ: Mapping[str, str]) -> str:
    try:
        conductor_url = environ["DBOS_CONDUCTOR_URL"]
    except KeyError:
        raise ValueError(_URL_ERROR) from None

    if (
        not isinstance(conductor_url, str)
        or not conductor_url
        or "?" in conductor_url
        or "#" in conductor_url
        or any(ord(character) <= 0x20 or ord(character) == 0x7F for character in conductor_url)
    ):
        raise ValueError(_URL_ERROR)

    try:
        endpoint = urlsplit(conductor_url)
        hostname = endpoint.hostname
        _ = endpoint.port  # Force validation of an explicitly supplied port.
    except (TypeError, UnicodeError, ValueError):
        raise ValueError(_URL_ERROR) from None

    if endpoint.scheme != "wss" or not hostname:
        raise ValueError(_URL_ERROR)
    return conductor_url


def configure_conductor(
    config: Mapping[str, Any], environ: Mapping[str, str] | None = None
) -> dict[str, Any]:
    """Return a copied DBOS Python config for ``DBOS(config=...)``.

    Connection values come only from the supplied environment (or ``os.environ``),
    and this helper does not alter TLS handling in the SDK.
    """
    source = os.environ if environ is None else environ
    conductor_url = _conductor_url(source)

    if config.get("conductor_metadata_only_mode", False):
        raise ValueError("Metadata-only mode is not supported by maestro MVP")

    conductor_key = source["DBOS_CONDUCTOR_KEY"]
    configured = dict(config)
    configured["conductor_url"] = conductor_url
    configured["conductor_key"] = conductor_key
    return configured
