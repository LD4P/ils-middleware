"""Updates localAdminMetadata with ILS Identifier."""

from airflow.providers.http.operators.http import SimpleHttpOperator

def UpdateIdentifier(**kwargs) -> SimpleHttpOperator:
    """Add Identifier to new Sinopia's localAdminMetadata."""
    conn_id = kwargs.get("conn_id")

    return SimpleHttpOperator(
        task_id="sinopia-id-update",
        http_conn_id=conn_id,
            

    )