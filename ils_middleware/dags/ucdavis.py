from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from ils_middleware.tasks.amazon.alma_s3 import get_from_alma_s3, send_to_alma_s3
from ils_middleware.tasks.amazon.sqs import SubscribeOperator, parse_messages
from ils_middleware.tasks.sinopia.local_metadata import new_local_admin_metadata
from ils_middleware.tasks.sinopia.email import (
    notify_and_log,
    send_update_success_emails,
)
from ils_middleware.tasks.sinopia.login import sinopia_login
from ils_middleware.tasks.sinopia.rdf2marc import Rdf2Marc
from ils_middleware.tasks.alma.post import NewMARCtoAlma


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
    "ucdavis",
    default_args=default_args,
    description="University of California Davis Alma DAG",
    schedule_interval=timedelta(minutes=5),
    start_date=datetime(2021, 8, 24),
    tags=["alma"],
    catchup=False,
    on_failure_callback=dag_failure_callback,
) as dag:
    # Monitors SQS for UC Davis queue
    # By default, SubscribeOperator will make the message available via XCom: "Get messages from an SQS queue and then
    # deletes the message from the SQS queue. If deletion of messages fails an AirflowException is thrown otherwise, the
    # message is pushed through XCom with the key 'messages'."
    # https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/_api/airflow/providers/amazon/aws/sensors/sqs/index.html
    listen_sns = SubscribeOperator(queue="ucdavis-ils")

    process_message = PythonOperator(
        task_id="sqs-message-parse",
        python_callable=parse_messages,
    )

    with TaskGroup(group_id="process_alma") as alma_task_group:
        run_rdf2marc = PythonOperator(
            task_id="rdf2marc",
            python_callable=Rdf2Marc,
            op_kwargs={
                "rdf2marc_lambda": Variable.get("rdf2marc_lambda"),
                "s3_bucket": Variable.get("marc_s3_bucket"),
            },
        )

        download_marc = PythonOperator(
            task_id="download_marc",
            python_callable=get_from_alma_s3,
        )

        export_marc_xml = PythonOperator(
            task_id="marc_xml_to_s3",
            python_callable=send_to_alma_s3,
        )

        alma_new_record = PythonOperator(
            task_id="post_new_alma",
            python_callable=NewMARCtoAlma,
            op_kwargs={
                "alma_api_key": Variable.get("ucdavis_alma_api_key"),
                "alma_import_profile_id": Variable.get("ucdavis_alma_import_profile_id")
            }
        )

        (run_rdf2marc >> download_marc >> export_marc_xml >> alma_new_record)
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
                "ils_tasks": {"ALMA": ["process_alma.post_new_alma"]},
            },
        )

        login_sinopia >> local_admin_metadata

    notify_sinopia_updated = PythonOperator(
        task_id="sinopia_update_success_notification",
        dag=dag,
        trigger_rule="none_failed",
        python_callable=send_update_success_emails,
    )

    processing_complete = DummyOperator(task_id="processing_complete", dag=dag)
    messages_received = DummyOperator(task_id="messages_received", dag=dag)
    messages_timeout = DummyOperator(task_id="sqs_timeout", dag=dag)


listen_sns >> [messages_received, messages_timeout]
messages_received >> process_message
process_message >> alma_task_group >> processed_sinopia
processed_sinopia >> sinopia_update_group >> notify_sinopia_updated
notify_sinopia_updated >> processing_complete
messages_timeout >> processing_complete
