"""Tests metadata checks"""

import datetime

import pytest
import requests  # type: ignore

from unittest.mock import MagicMock

from pytest_mock import MockerFixture

from airflow import DAG
from airflow.models.taskinstance import TaskInstance
from airflow.operators.dummy import DummyOperator

from ils_middleware.tasks.sinopia.metadata_check import (
    existing_metadata_check,
    _parse_date,
)


def test_task():
    start_date = datetime.datetime(2021, 10, 28)
    test_dag = DAG(
        "test_dag", default_args={"owner": "airflow", "start_date": start_date}
    )
    return DummyOperator(task_id="test", dag=test_dag)


task_instance = TaskInstance(test_task())

admin_metadata = [
    {
        "@id": "https://api.sinopia.io/resource/1234abcde",
        "http://id.loc.gov/ontologies/bibframe/identifier": [{"@id": "_:b1"}],
        "http://sinopia.io/vocabulary/exportDate": [
            {"@value": "2021-10-28T22:19:06.176422"}
        ],
    },
    {
        "@id": "_:b1",
        "http://www.w3.org/1999/02/22-rdf-syntax-ns#value": ["13704749"],
        "http://id.loc.gov/ontologies/bibframe/source": [{"@id": "_:b2"}],
    },
    {
        "@id": "_:b2",
        "http://www.w3.org/2000/01/rdf-schema#label": [{"@value": "SIRSI"}],
    },
]

sinopia_api = {
    # Happy Path
    "https://api.sinopia.io/resource/gh1234/relationships": {
        "bfAdminMetadataAllRefs": ["https://api.sinopia.io/resource/1234abcde"]
    },
    "https://api.sinopia.io/resource/1234abcde": {
        "templateId": "pcc:sinopia:localAdminMetadata",
        "data": admin_metadata,
    },
    # Not a localAdmin record
    "https://api.sinopia.io/resource/ku333aa555/relationships": {
        "bfAdminMetadataAllRefs": ["https://api.sinopia.io/resource/753878c"]
    },
    "https://api.sinopia.io/resource/753878c": {"templateId": "pcc:bf2:AdminMetadata"},
    # No bfAdminMetadata
    "https://api.sinopia.io/resource/oprt5531/relationships": {
        "bfAdminMetadataAllRefs": []
    },
    # Missing AdminMetadata URI
    "https://s.io/11ec/relationships": {
        "bfAdminMetadataAllRefs": ["https://s.io/3818"]
    },
}


@pytest.fixture
def mock_requests(monkeypatch, mocker: MockerFixture):
    def mock_get(*args, **kwargs):
        new_result = mocker.stub(name="get")
        if args[0] in sinopia_api:
            new_result.status_code = 200
            new_result.json = lambda: sinopia_api.get(args[0])
        else:
            new_result.status_code = 401
            new_result.text = "Not Found"
        return new_result

    monkeypatch.setattr(requests, "get", mock_get)


@pytest.fixture
def mock_datetime(monkeypatch):
    datetime_mock = MagicMock(wrap=datetime.datetime)
    datetime_mock.fromisoformat.return_value = datetime.datetime(2021, 10, 28, 22, 29)

    monkeypatch.setattr(datetime, "datetime", datetime_mock)


@pytest.fixture
def mock_task_instance(monkeypatch):
    def mock_xcom_push(*args, **kwargs):
        return []

    monkeypatch.setattr(TaskInstance, "xcom_push", mock_xcom_push)


def test_check_one_metadata_record(mock_requests, mock_datetime, mock_task_instance):
    result = existing_metadata_check(
        task_instance=task_instance,
        resource_uri="https://api.sinopia.io/resource/gh1234",
        ils_tasks={"overlay": "post_ils_overlay"},
    )
    assert result.startswith("post_ils_overlay")


def test_no_admin_metadata_records(mock_requests, mock_datetime, mock_task_instance):
    result = existing_metadata_check(
        task_instance=task_instance,
        resource_uri="https://api.sinopia.io/resource/oprt5531",
        ils_tasks={"new": "post_ils_new"},
    )

    assert result.startswith("post_ils_new")


def test_no_local_metadata_records(mock_requests, mock_datetime, mock_task_instance):
    result = existing_metadata_check(
        task_instance=task_instance,
        resource_uri="https://api.sinopia.io/resource/ku333aa555",
        ils_tasks={"new": "post_ils_new"},
    )

    assert result.startswith("post_ils_new")


def test_resource_uri_not_found(mock_requests, mock_datetime, mock_task_instance):

    with pytest.raises(
        Exception, match="https://s.io/relationships retrieval failed 401"
    ):
        existing_metadata_check(
            task_instance=task_instance, resource_uri="https://s.io"
        )


def test_metadata_uri_not_found(mock_requests, mock_datetime, mock_task_instance):
    with pytest.raises(Exception, match="https://s.io/3818 retrieval failed 401"):
        existing_metadata_check(
            task_instance=task_instance, resource_uri="https://s.io/11ec"
        )


def test_isoformat_export_date():
    timestamp = _parse_date("2021-11-17T16:30:54.904032")

    assert timestamp.year == 2021
    assert timestamp.month == 11
    assert timestamp.day == 17
    assert timestamp.hour == 16
    assert timestamp.minute == 30
    assert timestamp.second == 54


def test_isoformat_alt_export_date():
    timestamp = _parse_date("2021-11-16T07:37:00Z")

    assert timestamp.year == 2021
    assert timestamp.month == 11
    assert timestamp.day == 16
    assert timestamp.hour == 7
    assert timestamp.minute == 37
    assert timestamp.second == 0


def test_mmddyy_export_date():
    timestamp = _parse_date("11/15/2021")

    assert timestamp.year == 2021
    assert timestamp.month == 11
    assert timestamp.day == 15
    assert timestamp.second == 0


def test_failed_export_date_parse():
    bad_date = "11-27-02021"
    with pytest.raises(ValueError, match=f"no valid date format found for {bad_date}"):
        _parse_date(bad_date)
