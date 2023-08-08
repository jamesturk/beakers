import httpx
from pydantic import BaseModel, Field
import datetime
import asyncio


class HttpResponse(BaseModel):
    """
    Beaker data type that represents an HTTP response.
    """

    url: str
    status_code: int
    response_body: str
    retrieved_at: datetime.datetime = Field(default_factory=datetime.datetime.now)


class SteadyRateLimit:
    """
    Filter that limits the rate of items flowing through the pipeline.
    """

    def __init__(self, f_callable, sleep_seconds: float):
        self.callable = f_callable
        self.sleep_seconds = sleep_seconds

    async def __call__(self, item: BaseModel) -> BaseModel:
        await asyncio.sleep(self.sleep_seconds)
        return await self.callable(item)


class HttpRequest:
    """
    Filter that converts from a beaker with a URL to a beaker with an HTTP response.
    """

    def __init__(self, field: str = "url", *, follow_redirects: bool = True) -> None:
        """
        Args:
            field: The name of the field in the beaker that contains the URL.
            follow_redirects: Whether to follow redirects.
        """
        self.field = field
        self.follow_redirects = follow_redirects
        transport = httpx.AsyncHTTPTransport(retries=1)
        self.client = httpx.AsyncClient(transport=transport)

    async def __call__(self, item: BaseModel) -> HttpResponse:
        url = getattr(item, self.field)

        # async with self.client as client:
        response = await self.client.get(url, follow_redirects=self.follow_redirects)

        return HttpResponse(
            url=url,
            status_code=response.status_code,
            response_body=response.text,
        )
