import ssl
from gql import Client, gql as gql_query
from gql.transport.requests import RequestsHTTPTransport
from gql.transport.websockets import WebsocketsTransport

from .config import GRAPHQL_ENDPOINT, GRAPHQL_HEADERS, GRAPHQL_WS_ENDPOINT, GRAPHQL_VERIFY_TLS

ssl_context = ssl.create_default_context()
if not GRAPHQL_VERIFY_TLS:
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE


async def graphql_subscribe(subscription_str, variables=None):
    """Generic GraphQL subscription over WebSocket."""
    transport = WebsocketsTransport(
        url=GRAPHQL_WS_ENDPOINT,
        headers={"Sec-WebSocket-Protocol": "graphql-transport-ws"},
        ssl=ssl_context,
    )
    async with Client(transport=transport, fetch_schema_from_transport=True) as session:
        subscription = gql_query(subscription_str)
        async for result in session.subscribe(subscription, variable_values=variables):
            yield result


def dict_to_graphql_input(data):
    def convert(value):
        if isinstance(value, str):
            escaped = (
                value.replace("\\", "\\\\")
                .replace('"', '\\"')
                .replace("\n", "\\n")
                .replace("\r", "\\r")
                .replace("\t", "\\t")
            )
            return f'"{escaped}"'
        if value is None:
            return "null"
        if isinstance(value, bool):
            return "true" if value else "false"
        if isinstance(value, dict):
            return "{" + ", ".join(f"{k}: {convert(v)}" for k, v in value.items()) + "}"
        if isinstance(value, list):
            return "[" + ", ".join(convert(v) for v in value) + "]"
        return str(value)

    return "{" + ", ".join(f"{k}: {convert(v)}" for k, v in data.items()) + "}"


def send_merge_mutation(type_name: str, input_obj: dict):
    """Send merge() mutation for a GraphQL type."""
    input_literal = dict_to_graphql_input(input_obj)
    query = (
        "mutation { "
        f'  merge(type: "{type_name}", '
        f"input: {input_literal}"
        ") "
        "}"
    )

    transport = RequestsHTTPTransport(
        url=GRAPHQL_ENDPOINT,
        headers=GRAPHQL_HEADERS,
        verify=GRAPHQL_VERIFY_TLS,
        retries=3,
    )
    client = Client(transport=transport, fetch_schema_from_transport=False)
    return client.execute(gql_query(query))
