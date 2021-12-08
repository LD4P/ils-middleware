"""Maps Sinopia RDF to FOLIO Records."""
import logging

import rdflib

import ils_middleware.tasks.folio.mappings.bf_instance as bf_instance_map
import ils_middleware.tasks.folio.mappings.bf_work as bf_work_map

logger = logging.getLogger(__name__)

BF_TO_FOLIO_MAP = {
    "contributor.primary.Person": {
        "template": bf_work_map.primary_contributor,
        "uri": "work",
        "class": "bf:Person",
    },
    "dateOfPublication": {
        "template": bf_instance_map.date_of_publication,
        "uri": "instance",
    },
    "editions": {"template": bf_work_map.editions, "uri": "work"},
    "format.category": {
        "template": bf_instance_map.instance_format_category,
        "uri": "instance",
    },
    "format.term": {
        "template": bf_instance_map.instance_format_term,
        "uri": "instance",
    },
    "identifiers.isbn": {
        "template": bf_instance_map.identifier,
        "uri": "instance",
        "class": "bf:Isbn",
    },
    "identifiers.oclc": {
        "template": bf_instance_map.local_identifier,
        "uri": "instance",
    },
    "instance_type": {"template": bf_work_map.instance_type_id, "uri": "work"},
    "language": {"template": bf_work_map.language, "uri": "work"},
    "modeOfIssuanceId": {
        "template": bf_instance_map.mode_of_issuance,
        "uri": "instance",
    },
    "notes": {"template": bf_instance_map.note, "uri": "instance"},
    "physical_description.dimensions": {
        "template": bf_instance_map.physical_description_dimensions,
        "uri": "instance",
    },
    "physical_description.extent": {
        "template": bf_instance_map.physical_description_extent,
        "uri": "instance",
    },
    "publisher.name": {"template": bf_instance_map.publisher, "uri": "instance"},
    "publisher.place": {"template": bf_instance_map.place, "uri": "instance"},
    "subjects": {"template": bf_work_map.subject, "uri": "work"},
    "title": {
        "template": bf_instance_map.title,
        "uri": "instance",
        "class": "bf:Title",
    },
}

FOLIO_FIELDS = BF_TO_FOLIO_MAP.keys()


def _task_id(task_groups: str) -> str:
    task_id = "bf-graph"
    if len(task_groups) > 0:
        task_id = f"{task_groups}.{task_id}"
    return task_id


def _build_and_query_graph(**kwargs) -> list:
    bf_class = kwargs["bf_class"]
    instance_uri = kwargs["instance_uri"]
    task_instance = kwargs["task_instance"]
    template = kwargs["template"]
    uri_type = kwargs["uri_type"]
    task_groups = ".".join(kwargs["task_groups_ids"])

    query_params = {}
    task_id = _task_id(task_groups)

    if uri_type.startswith("bf_work"):
        query_params[uri_type] = task_instance.xcom_pull(
            key=instance_uri, task_ids=task_id
        ).get("work_uri")
    else:
        query_params[uri_type] = instance_uri
    if bf_class:
        query_params["bf_class"] = bf_class

    query = template.format(**query_params)
    graph = rdflib.Graph()
    json_ld = task_instance.xcom_pull(key=instance_uri, task_ids=task_id).get("graph")
    graph.parse(data=json_ld, format="json-ld")
    logger.debug(f"Graph size is {len(graph)}\n{query}")
    return [row for row in graph.query(query)]


def map_to_folio(**kwargs):
    task_instance = kwargs["task_instance"]
    folio_field = kwargs.get("folio_field")
    bf_class = BF_TO_FOLIO_MAP[folio_field].get("class")
    template = BF_TO_FOLIO_MAP[folio_field].get("template")
    uri_type = f"bf_{BF_TO_FOLIO_MAP[folio_field].get('uri')}"

    resources = task_instance.xcom_pull(key="resources", task_ids="sqs-message-parse")
    for instance_uri in resources:
        values = _build_and_query_graph(
            uri_type=uri_type,
            template=template,
            instance_uri=instance_uri,
            bf_class=bf_class,
            **kwargs,
        )
        task_instance.xcom_push(key=instance_uri, value=values)
    return "mapping_complete"
