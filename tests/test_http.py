import pytest
from pydantic import BaseModel
from databeakers.http import HttpRequest, HttpResponse


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
    assert "example" in response.response_body
    assert response.url == "http://example.com"


@pytest.mark.asyncio
async def test_http_request_alt_field_name():
    http_request = HttpRequest(field="alt_field_name")
    response = await http_request(AltURL(alt_field_name="http://example.com"))
    assert isinstance(response, HttpResponse)
    assert response.status_code == 200
    assert "example" in response.response_body
    assert response.url == "http://example.com"
    assert repr(http_request) == "HttpRequest(alt_field_name)"
