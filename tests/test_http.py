import pytest
from pydantic import BaseModel
from databeakers.pipeline import Pipeline
from databeakers.http import HttpRequest, HttpResponse, HttpEdge


class URL(BaseModel):
    url: str


class AltURL(BaseModel):
    alt_field_name: str


@pytest.mark.asyncio
async def test_http_request():
    http_request = HttpRequest()
    response = await http_request(URL(url="http://example.com"))
    assert isinstance(response, HttpResponse)
    assert response.status_code == 200
    assert "example" in response.text
    assert response.url == "http://example.com"


@pytest.mark.asyncio
async def test_http_request_alt_field_name():
    http_request = HttpRequest(field="alt_field_name")
    response = await http_request(AltURL(alt_field_name="http://example.com"))
    assert isinstance(response, HttpResponse)
    assert response.status_code == 200
    assert "example" in response.text
    assert response.url == "http://example.com"
    assert repr(http_request) == "HttpRequest(alt_field_name)"


def test_make_http_edge():
    # very simple test, could be improved
    pipeline = Pipeline("http", ":memory:")
    pipeline.add_beaker("start", URL)
    pipeline.add_beaker("response", HttpResponse)
    pipeline.add_out_transform("start", HttpEdge("response"))
    # ensure that default error beakers were added
    assert "http_timeout" in pipeline.beakers
    assert "http_error" in pipeline.beakers
