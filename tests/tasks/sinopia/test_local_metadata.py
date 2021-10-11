"""Test Sinopia LocalAdminMetadata"""

import rdflib

from ils_middleware.tasks.sinopia.local_metadata import create_admin_metadata


def test_create_admin_metadata():
    admin_metadata = rdflib.Graph()
    admin_metadata_str = create_admin_metadata(
        instance_url="https://api.sinopia.io/resource/12345",
        identifier_ils={"234566": "symphony"},
        cataloger_id="lstanford",
    )
    admin_metadata.parse(data=admin_metadata_str, format="json-ld")
    assert len(admin_metadata) == 10
    for row in admin_metadata.query(
        """SELECT ?ident WHERE {
       ?id rdf:type <http://id.loc.gov/ontologies/bibframe/Local> .
       ?id rdf:value ?ident .
    }"""
    ):
        assert str(row[0]) == "234566"
