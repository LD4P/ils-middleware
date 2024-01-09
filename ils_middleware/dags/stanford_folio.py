from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from ils_middleware.tasks.amazon.sqs import SubscribeOperator, parse_messages

from ils_middleware.tasks.sinopia.local_metadata import new_local_admin_metadata
from ils_middleware.tasks.sinopia.email import (
    notify_and_log,
    send_notification_emails,
)

from ils_middleware.tasks.sinopia.login import sinopia_login

# from ils_middleware.tasks.sinopia.metadata_check import existing_metadata_check
from ils_middleware.tasks.folio.build import build_records
from ils_middleware.tasks.folio.login import FolioLogin
from ils_middleware.tasks.folio.graph import construct_graph
from ils_middleware.tasks.folio.map import FOLIO_FIELDS, map_to_folio
from ils_middleware.tasks.folio.new import post_folio_records


def task_failure_callback(ctx_dict) -> None:
    notify_and_log("Error executing task", ctx_dict)


def dag_failure_callback(ctx_dict) -> None:
    notify_and_log("Error executing DAG", ctx_dict)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "provider": None,
    "provide_context": True,
    "on_failure_callback": task_failure_callback,
}

with DAG(
    "stanford_folio",
    default_args=default_args,
    description="Stanford FOLIO DAG",
    schedule_interval=timedelta(minutes=5),
    start_date=datetime(2021, 8, 24),
    tags=["folio", "stanford"],
    catchup=False,
    on_failure_callback=dag_failure_callback,
) as dag:
    # Monitors SQS for Stanford queue
    # By default, SubscribeOperator will make the message available via XCom: "Get messages from an SQS queue and then
    # deletes the message from the SQS queue. If deletion of messages fails an AirflowException is thrown otherwise, the
    # message is pushed through XCom with the key 'messages'."
    # https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/_api/airflow/providers/amazon/aws/sensors/sqs/index.html
    listen_sns = SubscribeOperator(queue="stanford-FOLIO")

    process_message = PythonOperator(
        task_id="sqs-message-parse",
        python_callable=parse_messages,
    )

    with TaskGroup(group_id="process_folio") as folio_task_group:
        folio_login = PythonOperator(
            task_id="folio-login",
            python_callable=FolioLogin,
            op_kwargs={
                "url": Variable.get("stanford_folio_auth_url"),
                "username": Variable.get("stanford_folio_login"),
                "password": Variable.get("stanford_folio_password"),
                "tenant": "sul",
            },
        )

        bf_graphs = PythonOperator(task_id="bf-graph", python_callable=construct_graph)

        with TaskGroup(group_id="folio_mapping") as folio_map_task_group:
            for folio_field in FOLIO_FIELDS:
                bf_to_folio = PythonOperator(
                    task_id=f"{folio_field}_task",
                    python_callable=map_to_folio,
                    op_kwargs={
                        "folio_field": folio_field,
                        "task_groups_ids": ["process_folio"],
                    },
                )

        folio_records = PythonOperator(
            task_id="build-folio",
            python_callable=build_records,
            op_kwargs={
                "task_groups_ids": ["process_folio", "folio_mapping"],
                "folio_url": Variable.get("stanford_folio_url"),
                "username": Variable.get("stanford_folio_login"),
                "password": Variable.get("stanford_folio_password"),
                "tenant": "sul",
            },
        )

        new_folio_records = PythonOperator(
            task_id="new-or-upsert-folio-records",
            python_callable=post_folio_records,
            op_kwargs={
                "folio_url": Variable.get("stanford_folio_url"),
                "endpoint": "/instance-storage/batch/synchronous?upsert=true",
                "tenant": "sul",
                "task_groups_ids": [
                    "process_folio",
                ],
                "token": "{{ task_instance.xcom_pull(key='return_value', task_ids='process_folio.folio-login')}}",
            },
        )

        bf_graphs >> folio_map_task_group
        folio_map_task_group >> [folio_records, folio_login] >> new_folio_records

    # Dummy Operator
    processed_sinopia = DummyOperator(
        task_id="processed_sinopia", dag=dag, trigger_rule="none_failed"
    )

    with TaskGroup(group_id="update_sinopia") as sinopia_update_group:

        # Sinopia Login
        login_sinopia = PythonOperator(
            task_id="sinopia-login",
            python_callable=sinopia_login,
            op_kwargs={
                "region": "us-west-2",
                "sinopia_env": Variable.get("sinopia_env"),
            },
        )

        # Adds localAdminMetadata
        local_admin_metadata = PythonOperator(
            task_id="sinopia-new-metadata",
            python_callable=new_local_admin_metadata,
            op_kwargs={
                "jwt": "{{ task_instance.xcom_pull(task_ids='update_sinopia.sinopia-login', key='return_value') }}",
                "ils_tasks": {
                    "SIRSI": [
                        "process_symphony.post_new_symphony",
                        "process_symphony.post_overlay_symphony",
                    ],
                    "FOLIO": ["process_folio.new-or-upsert-folio-records"],
                },
            },
        )

        login_sinopia >> local_admin_metadata

    notify_sinopia_updated = PythonOperator(
        task_id="sinopia_update_notification",
        dag=dag,
        trigger_rule="none_failed",
        python_callable=send_notification_emails,
    )

    processing_complete = DummyOperator(
        task_id="processing_complete", dag=dag, trigger_rule="one_success"
    )
    messages_received = DummyOperator(task_id="messages_received", dag=dag)
    messages_timeout = DummyOperator(
        task_id="sqs_timeout", dag=dag, trigger_rule="all_failed"
    )


listen_sns >> [messages_received, messages_timeout]
messages_received >> process_message
process_message >> folio_task_group >> processed_sinopia
processed_sinopia >> sinopia_update_group >> notify_sinopia_updated
notify_sinopia_updated >> processing_complete
messages_timeout >> processing_complete
