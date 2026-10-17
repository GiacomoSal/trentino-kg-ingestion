"""Stage 2: mapping (generic).

Builds the teleontology (the E-type graph of the application) from the sources listed
in pipeline.yaml. For every source that has its own ontology, the teleontology contains:
  - the classes, hierarchy and properties of that ontology;
  - the alignment axioms of its alignment file, which connect them to the unifying
    ontology with two operations, for classes and properties alike:
      equivalent  owl:equivalentClass / owl:equivalentProperty
      child_of    rdfs:subClassOf / rdfs:subPropertyOf
Sources already described with the unifying ontology (e.g. OSM) need no alignment.
The entities are not touched: they keep the types of their own graph.
"""
import sys

from rdflib import Graph, Namespace, URIRef
from rdflib.namespace import OWL, RDF, RDFS

from kg_utils import ensure_parent, expand, load_yaml

PIPELINE = "config/pipeline.yaml"
GEO = Namespace("http://www.w3.org/2003/01/geo/wgs84_pos#")

AXIOMS = {
    "classes": {"equivalent": OWL.equivalentClass, "child_of": RDFS.subClassOf},
    "properties": {"equivalent": OWL.equivalentProperty, "child_of": RDFS.subPropertyOf},
}


def check_exists(term: URIRef, ontology: Graph, where: str) -> None:
    if (term, None, None) not in ontology:
        print(f"  [WARNING] {term} is not defined in {where}")


def add_alignment(tele: Graph, name: str, source_ontology: Graph, alignment: dict, unified: Graph) -> int:
    """Adds the axioms of one alignment file; returns how many were added."""
    ns = alignment["namespaces"]
    added = 0
    for section, key in (("classes", "class"), ("properties", "property")):
        for rule in alignment.get(section, []):
            term = expand(rule[key], ns)
            target = expand(rule["target"], ns)
            predicate = AXIOMS[section].get(rule["operation"])
            if predicate is None:
                print(f"  [ERROR] unknown operation '{rule['operation']}' for {rule[key]}")
                sys.exit(1)
            check_exists(term, source_ontology, f"the {name} ontology")
            check_exists(target, unified, "the unifying ontology")
            tele.add((term, predicate, target))
            added += 1
    for prefix, uri in ns.items():
        tele.bind(prefix, uri, replace=True)
    return added


def main() -> None:
    cfg = load_yaml(PIPELINE)
    unified = Graph()
    unified.parse(cfg["unified_ontology"])
    # coordinates are stored with WGS84 geo:lat / geo:long, next to the unifying ontology
    for prop in (GEO.lat, GEO.long):
        unified.add((prop, RDF.type, OWL.DatatypeProperty))

    tele = Graph()
    tele.add((URIRef("http://knowdive.disi.unitn.it/trentino-teleontology"), RDF.type, OWL.Ontology))
    tele.add((GEO.lat, RDF.type, OWL.DatatypeProperty))
    tele.add((GEO.long, RDF.type, OWL.DatatypeProperty))
    tele.bind("geo", GEO, replace=True)

    for name, source in cfg["sources"].items():
        if not source.get("ontology"):
            print(f"  {name}: described with the unifying ontology, no alignment")
            continue
        ontology = Graph()
        ontology.parse(source["ontology"])
        # the E-type graph keeps the schema of the source (classes, hierarchy, properties)
        for triple in ontology:
            if triple[1] == RDF.type and triple[2] == OWL.Ontology:
                continue
            tele.add(triple)
        n = 0
        if source.get("alignment"):
            n = add_alignment(tele, name, ontology, load_yaml(source["alignment"]), unified)
        print(f"  {name}: {len(ontology)} schema triples, {n} alignment axioms")

    out = cfg["outputs"]["teleontology"]
    tele.serialize(destination=ensure_parent(out), format="turtle", encoding="utf-8")
    print(f"Teleontology saved to {out} ({len(tele)} triples)")


if __name__ == "__main__":
    main()
