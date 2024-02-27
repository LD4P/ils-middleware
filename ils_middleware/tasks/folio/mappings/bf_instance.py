"""SPARQL queries to extract data from a RDF Graph made up json-ld from Sinopia
BF Instance with its associated BF Work.
"""

instance_format_id = """PREFIX bf: <http://id.loc.gov/ontologies/bibframe/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?format_category ?format_term
WHERE {{
    <{bf_instance}> a bf:Instance .
    <{bf_instance}> bf:media ?format_category_uri .
    <{bf_instance}> bf:carrier ?format_term_uri .
    ?format_category_uri rdfs:label ?format_category .
    ?format_term_uri rdfs:label ?format_term .
}}
"""

isbn_identifier = """PREFIX bf: <http://id.loc.gov/ontologies/bibframe/>
PREFIX sinopia: <http://sinopia.io/vocabulary/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>


SELECT ?isbn
WHERE {{
   <{bf_instance}> a bf:Instance ;
        bf:identifiedBy ?id .
   ?id a bf:Isbn ;
        rdf:value ?isbn .
}}
"""

lccn_identifier = """PREFIX bf: <http://id.loc.gov/ontologies/bibframe/>
PREFIX sinopia: <http://sinopia.io/vocabulary/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?lccn
WHERE {{
   <{bf_instance}> a bf:Instance ;
        bf:identifiedBy ?id .
   ?id a bf:Lccn ;
        rdf:value ?lccn .
}}
"""

local_identifier = """PREFIX bf: <http://id.loc.gov/ontologies/bibframe/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?identifier
WHERE {{
    <{bf_instance}> a bf:Instance .
    <{bf_instance}> bf:identifiedBy ?ident_bnode .
    ?ident_bnode a bf:Local .
    ?ident_bnode bf:source ?source_bnode .
    ?ident_bnode rdf:value ?identifier .
    ?source_bnode a bf:Source .
    OPTIONAL {{
        ?source_bnode rdfs:label "OColC" .
    }}
    OPTIONAL {{
        ?source_bnode rdfs:label "OCLC" .
    }}
}}
"""

mode_of_issuance = """PREFIX bf: <http://id.loc.gov/ontologies/bibframe/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SElECT ?mode_of_issuance
WHERE {{
    <{bf_instance}> a bf:Instance .
    <{bf_instance}> bf:issuance ?mode_of_issuance_uri .
    ?mode_of_issuance_uri rdfs:label ?mode_of_issuance
}}
"""

note = """PREFIX bf: <http://id.loc.gov/ontologies/bibframe/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?note
WHERE {{
    <{bf_instance}> a bf:Instance .
    <{bf_instance}> bf:note ?note_bnode .
    ?note_bnode a bf:Note .
    ?note_bnode rdfs:label ?note .
}}
"""

physical_description = """PREFIX bf: <http://id.loc.gov/ontologies/bibframe/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?extent ?dimensions
WHERE {{
    <{bf_instance}> a bf:Instance .
    <{bf_instance}> bf:extent ?extent_bnode .
    ?extent_bnode a bf:Extent .
    ?extent_bnode rdfs:label ?extent .
    <{bf_instance}> bf:dimensions ?dimensions .
}}
"""

publication = """PREFIX bf: <http://id.loc.gov/ontologies/bibframe/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?publisher ?date ?place
WHERE {{
   <{bf_instance}> a bf:Instance .
   <{bf_instance}> bf:provisionActivity ?activity .
   ?activity a bf:Publication .
   ?activity bf:agent ?agent .
   ?agent a bf:Agent .
   ?agent rdfs:label ?publisher .
   OPTIONAL {{
      ?activity bf:date ?date .
   }}
   OPTIONAL {{
      ?activity bf:place ?place_holder .
      ?place_holder rdfs:label ?place .
   }}
}}
"""

title = """PREFIX bf: <http://id.loc.gov/ontologies/bibframe/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?main_title ?subtitle ?part_number ?part_name ?responsibility
WHERE {{
  <{bf_instance}> a bf:Instance .
  <{bf_instance}> bf:title ?title .
  ?title a {bf_class} .
  ?title bf:mainTitle ?main_title .
  OPTIONAL {{
     ?title bf:subtitle ?subtitle .
  }}
  OPTIONAL {{
     ?title bf:partNumber ?part_number .
  }}
  OPTIONAL {{
     ?title bf:partName ?part_name .
  }}
  OPTIONAL {{
     ?title  bf:responsibilityStatement ?responsibility .
  }}
}}
"""
