"""Custom Operator using AWS SQSSensor."""
import logging
import json

import requests  # type: ignore

from airflow.models import Variable
from airflow.providers.amazon.aws.sensors.sqs import SQSSensor

logger = logging.getLogger(__name__)


# Should return aws_sqs_sensor operator
# https://airflow.apache.org/docs/apache-airflow/1.10.12/_api/airflow/contrib/sensors/aws_sqs_sensor/index.html
def SubscribeOperator(**kwargs) -> SQSSensor:
    """Subscribe to a topic to filter SQS messages."""
    queue_name = kwargs.get("queue", "")
    aws_sqs_url = Variable.get("sqs_url")
    return SQSSensor(
        aws_conn_id="aws_sqs_connection",
        sqs_queue=f"{aws_sqs_url}{queue_name}",
        task_id="sqs-sensor",
        dag=kwargs.get("dag"),
        max_messages=10,
        timeout=240,
    )


def get_resource(resource_uri: str) -> dict:
    """Retrieves the Resource"""
    result = requests.get(resource_uri)
    if result.status_code < 400:
        return result.json()
    return {}


def parse_messages(**kwargs) -> str:
    """Parses SQS Message Body into constituent part."""
    task_instance = kwargs["task_instance"]
    raw_sqs_messages = task_instance.xcom_pull(key="messages", task_ids="sqs-sensor")
    resources = []
    resources_with_errors = []
    for message in raw_sqs_messages:
        message_body = json.loads(message.get("Body"))
        resource_uri = message_body["resource"]["uri"]
        try:
            resources.append(resource_uri)
            task_instance.xcom_push(
                key=resource_uri,
                value={
                    "email": message_body["user"]["email"],
                    "group": message_body["group"],
                    "target": message_body["target"],
                    "resource_uri": resource_uri,
                    "resource": get_resource(resource_uri),
                    "target_resource_id": message_body.get("targetResourceId"),                     
                },
            )
        except KeyError:
            resources_with_errors.append(resource_uri)

    task_instance.xcom_push(key="resources", value=resources)
    task_instance.xcom_push(key="bad_resources", value=resources_with_errors)
    return "completed_parse"
