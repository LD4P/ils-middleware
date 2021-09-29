from datetime import datetime, timedelta

from ils_middleware.tasks.amazon.s3 import get_from_s3, send_to_s3
from ils_middleware.tasks.amazon.sqs import SubscribeOperator
from ils_middleware.tasks.sinopia.sinopia import UpdateIdentifier
from ils_middleware.tasks.sinopia.rdf2marc import Rdf2Marc
from ils_middleware.tasks.symphony.login import SymphonyLogin
from ils_middleware.tasks.symphony.new import NewMARCtoSymphony
from ils_middleware.tasks.symphony.mod_json import to_symphony_json


from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup


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
}

with DAG(
    "stanford",
    default_args=default_args,
    description="Stanford Symphony and FOLIO DAG",
    schedule_interval=timedelta(minutes=5),
    start_date=datetime(2021, 8, 24),
    tags=["symphony", "folio"],
    catchup=False,
) as dag:
    # Monitors SQS for Stanford topic
    listen_sns = SubscribeOperator(topic="stanford")

    with TaskGroup(group_id="process_symphony") as symphony_task_group:

        run_rdf2marc = PythonOperator(
            task_id="rdf2marc",
            python_callable=Rdf2Marc,
        )

        download_marc = PythonOperator(
            task_id="download_marc",
            python_callable=get_from_s3,
        )

        export_marc_json = PythonOperator(
            task_id="marc_json_to_s3",
            python_callable=send_to_s3,
        )

        convert_to_symphony_json = PythonOperator(
            task_id="convert_to_symphony_json",
            python_callable=to_symphony_json,
        )

        # Symphony Dev Server Settings
        library_key = "GREEN"
        home_location = "STACKS"
        symphony_app_id = "SINOPIA_DEV"
        symphony_client_id = "SymWSStaffClient"
        symphony_conn_id = "symphony_dev"
        symphony_item_type = (
            "STKS-MONO"  # This could be mapped from the Instance RDF template
        )

        # Log in and retrieve token
        symphony_login = SymphonyLogin(
            app_id=symphony_app_id,
            client_id=symphony_client_id,
            conn_id=symphony_conn_id,
            login=Variable.get("stanford_symphony_dev_login"),
            password=Variable.get("stanford_symphony_dev_password"),
        )

        #  Send to Symphony Web API
        symphony_add_record = NewMARCtoSymphony(
            app_id=symphony_app_id,
            client_id=symphony_client_id,
            conn_id=symphony_conn_id,
            library_key=library_key,
            marc_json="{{ task_instance.xcom_pull(key='return_value', task_ids=['convert_to_symphony_json'])[0]}}",
            item_type=symphony_item_type,
            home_location=home_location,
            token="{{ task_instance.xcom_pull(key='return_value', task_ids=['symphony_login'])[0]}}",
        )

        run_rdf2marc >> download_marc  >> export_marc_json >> convert_to_symphony_json >> [ symphony_login >> symphony_add_record]


    with TaskGroup(group_id="process_folio") as folio_task_group:
        download_folio_marc = DummyOperator(task_id="download_folio_marc", dag=dag)

        export_folio_json = DummyOperator(task_id="folio_json_to_s3", dag=dag)

        send_to_folio = DummyOperator(task_id="folio_send", dag=dag)

        download_folio_marc >> export_folio_json >> send_to_folio

    # Dummy Operator
    processed_sinopia = DummyOperator(
        task_id="processed_sinopia", dag=dag, trigger_rule="none_failed"
    )

    # Updates Sinopia URLS with HRID
    update_sinopia = PythonOperator(
        task_id="sinopia-id-update",
        python_callable=UpdateIdentifier,
    )

listen_sns >> [symphony_task_group, folio_task_group] >> processed_sinopia
processed_sinopia >> update_sinopia
