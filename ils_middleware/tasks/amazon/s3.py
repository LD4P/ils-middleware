import json
import logging
import os
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from pymarc import MARCReader


def get_from_s3(**kwargs) -> dict:
    task_instance = kwargs["task_instance"]
    instance_id = task_instance.xcom_pull(task_ids="symphony_json")
    s3_hook = S3Hook(aws_conn_id="aws_lambda_connection")

    temp_file = s3_hook.download_file(
        key=f"marc/airflow/{instance_id}/record.mar",
        bucket_name="sinopia-marc-development",
    )

    return {"id": instance_id, "temp_file": temp_file}


def send_to_s3(**kwargs) -> None:
    s3_hook = S3Hook(aws_conn_id="aws_lambda_connection")
    instance = get_temp_instances(kwargs["task_instance"])
    marc_record = marc_record_from_temp_file(instance)

    s3_hook.load_string(
        marc_record.as_json(),
        f"marc/airflow/{instance['id']}/record.json",
        "sinopia-marc-development",
        replace=True,
    )
    # Cheat to get the JSON into XCOM
    return {"marc_json": json.dumps(marc_record.as_json())}


def get_temp_instances(task_instance):
    """Returns the temp file location from the prior task"""
    return task_instance.xcom_pull(task_ids="process_symphony.download_marc")


def marc_record_from_temp_file(instance):
    instance_id = instance["id"]
    temp_file = instance["temp_file"]
    if os.path.exists(temp_file) and os.path.getsize(temp_file) > 0:
        with open(temp_file, "rb") as marc:
            return next(MARCReader(marc))
    else:
        logging.error(f"MARC data for {instance_id} missing or empty.")
        raise Exception()
