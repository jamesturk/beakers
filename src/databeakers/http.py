import httpx
from pydantic import BaseModel, Field
import datetime


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

    def __init__(self, field: str = "url"):
        """
        Args:
            field: The name of the field in the beaker that contains the URL.
        """
        self.field = field

    async def __call__(self, item: BaseModel) -> HttpResponse:
        url = getattr(item, self.field)

        async with httpx.AsyncClient() as client:
            response = await client.get(url)

        return HttpResponse(
            url=url,
            status_code=response.status_code,
            response_body=response.text,
        )
