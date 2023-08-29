import httpx
from pydantic import BaseModel, Field
import datetime
from .edges import Transform


class HttpResponse(BaseModel):
    """
    Beaker data type that represents an HTTP response.
    """

    url: str
    status_code: int
    text: str
    retrieved_at: datetime.datetime = Field(default_factory=datetime.datetime.now)


class HttpRequest:
    """
    Filter that converts from a beaker with a URL to a beaker with an HTTP response.
    """

    def __init__(
        self,
        field: str = "url",
        *,
        follow_redirects: bool = True,
        retries: int = 0,
        headers: dict[str, str] | None = None,
    ) -> None:
        """
        Args:
            field: The name of the field in the beaker that contains the URL.
            follow_redirects: Whether to follow redirects.
        """
        self.field = field
        self.follow_redirects = follow_redirects
        transport = httpx.AsyncHTTPTransport(retries=retries)
        self.client = httpx.AsyncClient(transport=transport, headers=headers)

    def __repr__(self):
        return f"HttpRequest({self.field})"

    async def __call__(self, item: BaseModel) -> HttpResponse:
        url = getattr(item, self.field)
        response = await self.client.get(url, follow_redirects=self.follow_redirects)
        # a more complicated edge could save the response, this one just raises
        # if there's an error which is fine because most of the time we don't
        # care about the response body (TODO: revisit this)
        response.raise_for_status()

        return HttpResponse(
            url=url,
            status_code=response.status_code,
            text=response.text,
        )


class HttpEdge(Transform):
    """
    Edge that converts from a beaker with a URL to a beaker with an HTTP response.
    """

    def __init__(
        self,
        to_beaker: str,
        *,
        name: str = "http",
        field: str = "url",
        follow_redirects: bool = True,
        retries: int = 0,
        error_beaker: str = "http_error",
        timeout_beaker: str = "http_timeout",
        error_map: dict[tuple[type[Exception], ...], str] | None = None,
    ) -> None:
        """
        Args:
            name: The name of the edge.
            to_beaker: The name of the beaker to convert to.
            field: The name of the field in the beaker that contains the URL.
            follow_redirects: Whether to follow redirects.
        """
        if error_map is None:
            error_map = {}
        error_map[(httpx.TimeoutException,)] = timeout_beaker
        error_map[(httpx.HTTPError,)] = error_beaker

        super().__init__(
            name=name,
            to_beaker=to_beaker,
            func=HttpRequest(
                field=field, follow_redirects=follow_redirects, retries=retries
            ),
            error_map=error_map,
            whole_record=False,
            allow_filter=False,
        )
