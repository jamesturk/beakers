import httpx
from pydantic import BaseModel, Field
import datetime
from ._models import Edge, EdgeType


class HttpResponse(BaseModel):
    """
    Beaker data type that represents an HTTP response.
    """

    url: str
    status_code: int
    response_body: str
    retrieved_at: datetime.datetime = Field(default_factory=datetime.datetime.now)


class HttpRequest:
    """
    Filter that converts from a beaker with a URL to a beaker with an HTTP response.
    """

    def __init__(
        self, field: str = "url", *, follow_redirects: bool = True, retries: int = 0
    ) -> None:
        """
        Args:
            field: The name of the field in the beaker that contains the URL.
            follow_redirects: Whether to follow redirects.
        """
        self.field = field
        self.follow_redirects = follow_redirects
        transport = httpx.AsyncHTTPTransport(retries=retries)
        self.client = httpx.AsyncClient(transport=transport)

    def __repr__(self):
        return f"HttpRequest({self.field})"

    async def __call__(self, item: BaseModel) -> HttpResponse:
        url = getattr(item, self.field)
        response = await self.client.get(url, follow_redirects=self.follow_redirects)

        return HttpResponse(
            url=url,
            status_code=response.status_code,
            response_body=response.text,
        )


def make_http_edge(
    name,
    *,
    whole_record: bool = False,
    # HttpRequest args
    field: str = "url",
    follow_redirects: bool = True,
    retries: int = 0,
    error_beaker: str = "http_error",
    timeout_beaker: str = "http_timeout",
) -> Edge:
    return Edge(
        name=name,
        func=HttpRequest(
            field=field, follow_redirects=follow_redirects, retries=retries
        ),
        error_map={
            (httpx.TimeoutException,): timeout_beaker,
            (httpx.HTTPError,): error_beaker,
        },
        edge_type=EdgeType.transform,
        whole_record=whole_record,
    )
