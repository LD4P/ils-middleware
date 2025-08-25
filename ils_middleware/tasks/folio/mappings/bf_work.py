"""SPARQL queries to extract data from a RDF Graph made up json-ld from Sinopia
BF Instance with its associated BF Work.
"""

contributor = """PREFIX bf: <http://id.loc.gov/ontologies/bibframe/>
PREFIX bflc: <http://id.loc.gov/ontologies/bflc/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?agent ?role
WHERE {{
    <{bf_work}> a bf:Work ;
          bf:contribution ?contrib_bnode .
    ?contrib_bnode a bf:Contribution .
    ?contrib_bnode bf:role ?role_uri .
    ?role_uri rdfs:label ?role .
    ?contrib_bnode bf:agent ?agent_uri .
    ?agent_uri a {bf_class} .
    ?agent_uri rdfs:label ?agent .
}}
"""

editions = """PREFIX bf: <http://id.loc.gov/ontologies/bibframe/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?edition
WHERE {{
    <{bf_work}> a bf:Work .
    <{bf_work}> bf:editionStatement ?edition .
}}
"""

instance_type_id = """PREFIX bf: <http://id.loc.gov/ontologies/bibframe/>

SELECT ?instance_type_id
WHERE {{
    <{bf_work}> a bf:Work .
    <{bf_work}> bf:content ?instance_type .
    ?instance_type rdfs:label ?instance_type_id .
}}
"""

issn_identifier = """PREFIX bf: <http://id.loc.gov/ontologies/bibframe/>
PREFIX sinopia: <http://sinopia.io/vocabulary/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?issn
WHERE {{
   <{bf_work}> a bf:Work ;
   bf:identifiedBy ?id .
   ?id a bf:Issn ;
   rdf:value ?issn .
 }}
 """

language = """PREFIX bf: <http://id.loc.gov/ontologies/bibframe/>

SELECT ?language_uri ?language
WHERE {{
    <{bf_work}> a bf:Work .
    <{bf_work}> bf:language ?language_uri .
    ?language_uri rdfs:label ?language .
}}
"""

primary_contributor = """PREFIX bf: <http://id.loc.gov/ontologies/bibframe/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?agent ?role
WHERE {{
    <{bf_work}> a bf:Work .
    <{bf_work}> bf:contribution ?contrib_bnode .
    ?contrib_bnode a bflc:PrimaryContribution .
    ?contrib_bnode bf:role ?role_uri .
    ?role_uri rdfs:label ?role .
    ?contrib_bnode bf:agent ?agent_uri .
    ?agent_uri a {bf_class} .
    ?agent_uri rdfs:label ?agent .
}}
"""

person_contributor = """PREFIX bf: <http://id.loc.gov/ontologies/bibframe/>
PREFIX sinopia: <http://sinopia.io/vocabulary/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?subject ?person_contribution ?role
WHERE {
    <{bf_work}> a bf:Work .
    <{bf_work}> bf:contribution ?contribution .
    ?contribution bf:agent ?pcontribution .
    ?pcontribution a bf:Person .
    ?pcontribution rdfs:label ?person_contribution .
    ?contribution bf:role ?type .
    ?type rdfs:label ?role
 }}
"""

corporate_contributor = """PREFIX bf: <http://id.loc.gov/ontologies/bibframe/>
PREFIX sinopia: <http://sinopia.io/vocabulary/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?subject ?corporate_contribution ?role
WHERE {
    <{bf_work}> a bf:Work .
    <{bf_work}> bf:contribution ?contribution .
    ?contribution bf:agent ?ccontribution .
    ?ccontribution a bf:Organization .
    ?ccontribution rdfs:label ?corporate_contribution .
    ?contribution bf:role ?type .
    ?type rdfs:label ?role .
}}
"""

subject = """PREFIX bf: <http://id.loc.gov/ontologies/bibframe/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?subject
WHERE {{
    <{bf_work}> a bf:Work .
    <{bf_work}> bf:subject ?subject_node .
    OPTIONAL {{
        ?subject_node rdfs:label ?subject .
    }}
}}
"""
