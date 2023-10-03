"""POST Instance to Alma API"""
import logging
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from urllib.parse import urlparse
from os import path
import requests
import lxml.etree as ET


logger = logging.getLogger(__name__)


def get_env_vars(institution):
    uri_region = Variable.get(f"alma_uri_region_{institution}")
    alma_api_key = Variable.get(f"alma_api_key_{institution}")
    return uri_region, alma_api_key


def parse_400(result):
    xml_response = ET.fromstring(result)
    xslt = ET.parse("ils_middleware/tasks/alma/xslt/put_mms_id.xsl")
    transform = ET.XSLT(xslt)
    result_tree = transform(xml_response)
    put_mms_id_str = str(result_tree)
    logger.debug(f"put_mms_id_str: {put_mms_id_str}")
    return put_mms_id_str


def NewInstancetoAlma(**kwargs):
    institution = kwargs["dag"].dag_id
    uri_region, alma_api_key = get_env_vars(institution)
    s3_hook = S3Hook(aws_conn_id="aws_lambda_connection")
    task_instance = kwargs.get("task_instance")
    resources = task_instance.xcom_pull(key="resources", task_ids="sqs-message-parse")

    for instance_uri in resources:
        instance_path = urlparse(instance_uri).path
        instance_id = path.split(instance_path)[-1]

    temp_file = s3_hook.download_file(
        key=f"/alma/{instance_id}/bfinstance_alma.xml",
        bucket_name=Variable.get("marc_s3_bucket"),
    )
    task_instance.xcom_pull(key=instance_uri, task_ids=temp_file)
    with open(temp_file, "rb") as f:
        data = f.read()
        logger.debug(f"file data: {data}")

        alma_uri = (
            uri_region
            + "/almaws/v1/bibs?"
            + "from_nz_mms_id=&from_cz_mms_id=&normalization=&validate=false"
            + "&override_warning=true&check_match=false&import_profile=&apikey="
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
        result = alma_result.content
        status = alma_result.status_code
        if status == 200:
            xml_response = ET.fromstring(result)
            mms_id = xml_response.xpath("//mms_id/text()")
            task_instance.xcom_push(key=instance_uri, value=mms_id)
        elif status == 400:
            # run xslt on the result in case the response is 400 and we need to update the record
            put_mms_id_str = parse_400(result)
            alma_update_uri = (
                uri_region
                + "/almaws/v1/bibs/"
                + put_mms_id_str
                + "?normalization=&validate=false&override_warning=true"
                + "&override_lock=true&stale_version_check=false&cataloger_level=&check_match=false"
                + "&apikey="
                + alma_api_key
            )
            putInstanceToAlma(
                alma_update_uri,
                data,
                task_instance,
                instance_uri,
            )
        else:
            raise Exception(f"Unexpected status code from Alma API: {status}")


def putInstanceToAlma(
    alma_update_uri,
    data,
    task_instance,
    instance_uri,
):
    put_update = requests.put(
        alma_update_uri,
        headers={
            "Content-Type": "application/xml; charset=UTF-8",
            "Accept": "application/xml",
        },
        data=data,
    )
    logger.debug(f"put update: {put_update.status_code}\n{put_update.text}")
    put_update_status = put_update.status_code
    result = put_update.content
    xml_response = ET.fromstring(result)
    put_mms_id = xml_response.xpath("//mms_id/text()")
    match put_update_status:
        case 200:
            task_instance.xcom_push(key=instance_uri, value=put_mms_id)
        case 500:
            raise Exception(f"Internal server error from Alma API: {put_update_status}")
        case _:
            raise Exception(
                f"Unexpected status code from Alma API: {put_update_status}"
            )
