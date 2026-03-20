import ssl
from gql import Client, gql as gql_query
from gql.transport.requests import RequestsHTTPTransport
from gql.transport.websockets import WebsocketsTransport

from .config import GRAPHQL_ENDPOINT, GRAPHQL_HEADERS, GRAPHQL_WS_ENDPOINT, GRAPHQL_VERIFY_TLS

ssl_context = ssl.create_default_context()
if not GRAPHQL_VERIFY_TLS:
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE

_http_transport: RequestsHTTPTransport | None = None
_http_client: Client | None = None


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


def _build_merge_field(type_name: str, input_obj: dict, alias: str | None = None) -> str:
    input_literal = dict_to_graphql_input(input_obj)
    prefix = f"{alias}: " if alias else ""
    return f'{prefix}merge(type: "{type_name}", input: {input_literal})'


def _get_http_client() -> Client:
    global _http_transport, _http_client
    if _http_client is not None:
        return _http_client

    _http_transport = RequestsHTTPTransport(
        url=GRAPHQL_ENDPOINT,
        headers=GRAPHQL_HEADERS,
        verify=GRAPHQL_VERIFY_TLS,
        retries=3,
    )
    _http_client = Client(transport=_http_transport, fetch_schema_from_transport=False)
    return _http_client


def send_merge_mutation(type_name: str, input_obj: dict):
    """Send merge() mutation for a GraphQL type."""
    query = f"mutation {{ {_build_merge_field(type_name, input_obj)} }}"
    return _get_http_client().execute(gql_query(query))


def send_merge_mutations_batch(type_name: str, input_objs: list[dict]):
    """Send many merge() mutations in a single GraphQL request using aliases."""
    if not input_objs:
        return {}

    fields = [_build_merge_field(type_name, input_obj, alias=f"m{i}") for i, input_obj in enumerate(input_objs)]
    query = "mutation { " + " ".join(fields) + " }"
    return _get_http_client().execute(gql_query(query))


def send_add_results_mutation(input_objs: list[dict]):
    """Send bulk addResults() mutation using GraphQL variables."""
    if not input_objs:
        return {}

    query = gql_query(
        """
        mutation AddResults($input: [JSON!]!) {
          addResults(input: $input)
        }
        """
    )
    return _get_http_client().execute(query, variable_values={"input": input_objs})
