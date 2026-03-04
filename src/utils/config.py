import os

GRAPHQL_ENDPOINT = os.getenv("GRAPHQL_ENDPOINT", "https://localhost:8888/uk-data/graphql")
GRAPHQL_HEADERS = {
    "Content-Type": "application/json",
}

GRAPHQL_WS_ENDPOINT = os.getenv("GRAPHQL_WS_ENDPOINT", "wss://localhost:8888/uk-data/graphql")


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


# Use TLS verification by default (works with public CAs such as Let's Encrypt).
# Set GRAPHQL_VERIFY_TLS=false for local/self-signed environments.
GRAPHQL_VERIFY_TLS = _env_bool("GRAPHQL_VERIFY_TLS", True)
