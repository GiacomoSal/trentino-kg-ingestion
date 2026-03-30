# SPARQL queries for ontology management

def get_class_existence_query(class_uri):
    return f"""
    ASK {{ <{class_uri}> a owl:Class . }}
    """

def get_property_existence_query(prop_uri):
    return f"""
    ASK {{ <{prop_uri}> a ?type . FILTER(?type IN (owl:ObjectProperty, owl:DatatypeProperty, rdf:Property)) }}
    """

def get_class_hierarchy_query(class_uri):
    return f"""
    CONSTRUCT {{
        ?c ?p ?o .
    }}
    WHERE {{
        <{class_uri}> rdfs:subClassOf* ?c .
        ?c ?p ?o .
        FILTER(
        ?p IN (
            rdf:type, rdfs:subClassOf, rdfs:label, rdfs:comment,
            owl:equivalentClass, owl:disjointWith, owl:intersectionOf, owl:unionOf
        )
        )
    }}
    """
def get_property_domain(prop_uri):
    return f"""
    SELECT DISTINCT ?domain WHERE {{
      <{prop_uri}> rdfs:domain ?domain .
    }}
    """

def get_property_range(prop_uri):
    return f"""
    SELECT DISTINCT ?range WHERE {{
      <{prop_uri}> rdfs:range ?range .
    }}
    """


def get_base_property(prop_uri):
    return f"""
    CONSTRUCT {{
      <{prop_uri}> ?p ?o .
    }}
    WHERE
    {{
        SELECT DISTINCT ?p ?o WHERE {{
          <{prop_uri}> ?p ?o .
        }}
      }}
    """

def get_entity_by_suffix_query(suffix):
    return f"""
    SELECT ?entity WHERE {{
      ?entity ?p ?o .
      FILTER(STRENDS(STR(?entity), "{suffix}"))
    }}
    LIMIT 1
    """

def get_insert_data_query(schema_uri, xsd_uri, triples):
    return f"""
    PREFIX schema: <{schema_uri}>
    PREFIX xsd: <{xsd_uri}>
    INSERT DATA {{
        {' '.join(triples)}
    }}
    """