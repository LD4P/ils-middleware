"""Updates localAdminMetadata with ILS Identifier."""

from airflow.models import Variable
from airflow.providers.http.operators.http import SimpleHttpOperator


def AddLocalAdminMetadata(**kwargs) -> SimpleHttpOperator:
    """Adds Sinopia's localAdminMetadata to Instance."""
    task_id = kwargs.get("task_id", "sinopia-id-update")
    conn_id = kwargs.get("conn_id")
    group = kwargs.get("group")
    record = kwargs.get("record")
    jwt = kwargs.get("jwt")
    sinopia_user = Variable.get("sinopia_user")

    headers = {"Authorization": f"Bearer {jwt}", "Content-Type": "application/json"}

    sinopia_doc = {
        "data": record,
        "user": sinopia_user,
        "group": group,
        "editGroups": [],
        "templateId": "pcc:sinopia:localAdminMetadata",
        "types": [],
        "bfAdminMetadataRefs": [],
        "bfItemRefs": [],
        "bfInstanceRefs": [],
        "bfWorkRefs": [],
    }

    return SimpleHttpOperator(
        task_id=task_id, http_conn_id=conn_id, headers=headers, data=sinopia_doc
    )
