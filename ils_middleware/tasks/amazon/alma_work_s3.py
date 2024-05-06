import logging
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from rdflib import Graph, URIRef, Namespace
from rdflib.namespace import RDF
from copy import deepcopy
from lxml import etree as ET
from ils_middleware.tasks.amazon.alma_ns import alma_namespaces

logger = logging.getLogger(__name__)


def send_work_to_alma_s3(**kwargs):
    s3_hook = S3Hook(aws_conn_id="aws_lambda_connection")
    task_instance = kwargs.get("task_instance")
    resources = task_instance.xcom_pull(key="resources", task_ids="sqs-message-parse")

    for instance_uri in resources:
        instance_uri = URIRef(instance_uri)
        work_uri = None
        instance_graph = Graph()
        work_graph = Graph()
        instance_graph.parse(instance_uri)
        # Define the bf namespace
        bf = Namespace("http://id.loc.gov/ontologies/bibframe/")
        for prefix, url in alma_namespaces:
            work_graph.bind(prefix, url)
        work_uri = instance_graph.value(
            subject=URIRef(instance_uri), predicate=bf.instanceOf
        )
        work_uri = URIRef(work_uri)
        # Explicitly state that work_uri is of type bf:Work
        work_graph.add((work_uri, RDF.type, bf.Work))
        # parse the work graph
        work_graph.parse(work_uri)
        # add the instance to the work graph
        work_graph.add((work_uri, bf.hasInstance, URIRef(instance_uri)))

        # serialize the work graph
        bfwork_alma_xml = work_graph.serialize(format="pretty-xml", encoding="utf-8")
        tree = ET.fromstring(bfwork_alma_xml)

        # Define namespaces
        namespaces = {
            "bf": "http://id.loc.gov/ontologies/bibframe/",
            "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
            "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
        }

        # Find all bf:Work elements
        works = tree.xpath("//bf:Work", namespaces=namespaces)

        for work in works:
            work_about = work.attrib[
                "{http://www.w3.org/1999/02/22-rdf-syntax-ns#}about"
            ]

            # Find the bf:relatedTo element with the same rdf:resource attribute value
            related_to = tree.xpath(
                f'//bf:relatedTo[@rdf:resource="{work_about}"]', namespaces=namespaces
            )

            if related_to:
                # Remove the rdf:resource attribute from the bf:relatedTo element
                related_to[0].attrib.pop(
                    "{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource", None
                )

                # Clone the bf:Work element and append it under the bf:relatedTo element
                cloned_work = deepcopy(work)
                related_to[0].append(cloned_work)

                # Remove the original bf:Work element that was cloned
                work.getparent().remove(work)

        # apply xslt to normalize work
        xslt = ET.parse("ils_middleware/tasks/amazon/xslt/normalize-work.xsl")
        transform = ET.XSLT(xslt)
        bfwork_alma_xml = transform(tree)
        bfwork_alma_xml = ET.tostring(
            bfwork_alma_xml, pretty_print=True, encoding="utf-8"
        )
        logger.info(f"Normalized BFWork description for {work_uri}.")
        # post to s3 as bytes
        s3_hook.load_bytes(
            bfwork_alma_xml,
            f"alma/{instance_uri}/bfwork_alma.xml",
            Variable.get("marc_s3_bucket"),
            replace=True,
        )
        # save to xcom
        task_instance.xcom_push(key=instance_uri, value=bfwork_alma_xml.decode("utf-8"))
        logger.info(f"Saved BFWork description(s) for {work_uri} to alma.")
        logger.info(f"bf_work_alma_xml: {bfwork_alma_xml.decode('utf-8')}")
