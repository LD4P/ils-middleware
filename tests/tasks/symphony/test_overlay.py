import pytest

import requests  # type: ignore

from airflow.hooks.base_hook import BaseHook
from pytest_mock import MockerFixture

from ils_middleware.tasks.symphony.overlay import overlay_marc_in_symphony

MARC_JSON = """{"leader": "11222999   adf", "fields": [{"tag": "245"}]}"""
CATKEY = "320011"


@pytest.fixture
def mock_connection(monkeypatch, mocker: MockerFixture):
    def mock_get_connection(*args, **kwargs):
        connection = mocker.stub(name="Connection")
        connection.host = "https://symphony.test.edu/"

        return connection

    monkeypatch.setattr(BaseHook, "get_connection", mock_get_connection)


@pytest.fixture
def mock_new_request(monkeypatch, mocker: MockerFixture):
    def mock_put(*args, **kwargs):
        new_result = mocker.stub(name="put_result")
        new_result.token = "234566"
        new_result.status_code = 201
        new_result.text = "Successful modified"
        new_result.json = lambda: {"@key": CATKEY}
        return new_result

    monkeypatch.setattr(requests, "put", mock_put)


def test_overlay_marc_in_symphony(mock_new_request, mock_connection):
    task_result = overlay_marc_in_symphony(
        conn_id="symphony_dev_login",
        session_token="abcde4590",
        catkey=CATKEY,
        marc_json=MARC_JSON,
    )

    assert task_result.startswith(CATKEY)


def test_missing_catkey(mock_new_request, mock_connection):

    with pytest.raises(ValueError, match="Catalog ID is required"):
        overlay_marc_in_symphony(
            conn_id="symphony_dev_login", session_token="abcde4590", marc_json=MARC_JSON
        )
