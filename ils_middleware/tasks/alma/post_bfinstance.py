"""POST Instance function for Alma API"""
import logging
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from urllib.parse import urlparse
from os import path
import requests
import lxml.etree as ET

logger = logging.getLogger(__name__)


def NewInstancetoAlma(**kwargs):
    s3_hook = S3Hook(aws_conn_id="aws_lambda_connection")
    task_instance = kwargs.get("task_instance")
    resources = task_instance.xcom_pull(key="resources", task_ids="sqs-message-parse")

    for instance_uri in resources:
        instance_path = urlparse(instance_uri).path
        instance_id = path.split(instance_path)[-1]

    temp_file = s3_hook.download_file(
        key=f"marc/alma/{instance_id}/bfinstance_alma.xml",
        bucket_name=Variable.get("marc_s3_bucket"),
    )

    task_instance.xcom_pull(key=instance_uri, task_ids=temp_file)
    data = open(temp_file, "rb").read()
    logger.debug(f"file data: {data}")

    alma_api_key = Variable.get("alma_sandbox_api_key")
    alma_import_profile_id = Variable.get("import_profile_id")

    alma_uri = (
        "https://api-na.hosted.exlibrisgroup.com/almaws/v1/bibs?"
        + "from_nz_mms_id=&from_cz_mms_id=&normalization=&validate=false"
        + "&override_warning=true&check_match=false&import_profile="
        + alma_import_profile_id
        + "&apikey="
        + alma_api_key
    )
    # post to alma
    alma_result = requests.post(
        alma_uri,
        headers={
            "Content-Type": "application/xml; charset=utf-8",
            "Accept": "application/xml",
            "x-api-key": alma_api_key,
        },
        data=data,
    )
    logger.debug(f"alma result: {alma_result.status_code}\n{alma_result.text}")

    # push the mms_id from the Alma API result to the next task, Sinopia Metadata Update
    result = alma_result.content
    xml_response = ET.fromstring(result)
    mms_id_list = xml_response.xpath("//mms_id/text()")
    mms_id = " ".join(mms_id_list)
    logger.debug(f"mms_id: {mms_id}")
    task_instance.xcom_push(key=instance_uri, value=mms_id)
