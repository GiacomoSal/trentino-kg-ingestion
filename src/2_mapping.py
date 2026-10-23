"""Stage 2: mapping (generic) -> teleontology.

Builds the teleontology, the E-type graph of the application, from the sources listed
in config/pipeline.yaml. For every source it takes, from the source ontology:
  - the e-types the application uses and all their ancestors (the hierarchy);
  - the data and object properties whose domain is one of those classes.
Then it adds the alignment axioms of the source's alignment file, which connect its
e-types and properties to the unifying ontology with two operations, for classes and
properties alike:
    equivalent  owl:equivalentClass / owl:equivalentProperty
    child_of    rdfs:subClassOf / rdfs:subPropertyOf
together with the unified classes those axioms point to and their ancestors.
Sources described with the unifying ontology itself (e.g. OSM) need no alignment.
"""
import sys
from typing import Iterable, Set

from rdflib import Graph, Namespace, URIRef
from rdflib.namespace import OWL, RDF, RDFS

from kg_utils import ensure_parent, expand, load_yaml

PIPELINE = "config/pipeline.yaml"
GEO = Namespace("http://www.w3.org/2003/01/geo/wgs84_pos#")

AXIOMS = {
    "classes": {"equivalent": OWL.equivalentClass, "child_of": RDFS.subClassOf},
    "properties": {"equivalent": OWL.equivalentProperty, "child_of": RDFS.subPropertyOf},
}
PROPERTY_TYPES = (OWL.ObjectProperty, OWL.DatatypeProperty)


def check_exists(term: URIRef, ontology: Graph, where: str) -> None:
    if (term, None, None) not in ontology:
        print(f"  [WARNING] {term} is not defined in {where}")


def with_ancestors(classes: Iterable[URIRef], ontology: Graph) -> Set[URIRef]:
    """The classes plus all their superclasses in the ontology."""
    seen, todo = set(classes), list(classes)
    while todo:
        for parent in ontology.objects(todo.pop(), RDFS.subClassOf):
            if isinstance(parent, URIRef) and parent not in seen:
                seen.add(parent)
                todo.append(parent)
    return seen


def add_slice(tele: Graph, ontology: Graph, classes: Set[URIRef]) -> int:
    """Copies the given classes (hierarchy, labels) and the properties whose domain is one of them."""
    for c in classes:
        tele.add((c, RDF.type, OWL.Class))
        for p in (RDFS.label, RDFS.comment):
            for value in ontology.objects(c, p):
                tele.add((c, p, value))
        for parent in ontology.objects(c, RDFS.subClassOf):
            if parent in classes:
                tele.add((c, RDFS.subClassOf, parent))
    n_props = 0
    for kind in PROPERTY_TYPES:
        for prop in ontology.subjects(RDF.type, kind):
            domains = [d for d in ontology.objects(prop, RDFS.domain) if d in classes]
            if not domains:
                continue
            n_props += 1
            tele.add((prop, RDF.type, kind))
            for d in domains:
                tele.add((prop, RDFS.domain, d))
            for p in (RDFS.range, RDFS.label, RDFS.comment):
                for value in ontology.objects(prop, p):
                    tele.add((prop, p, value))
    return n_props


def add_alignment(tele: Graph, name: str, source_ontology: Graph, alignment: dict, unified: Graph) -> Set[URIRef]:
    """Adds the axioms of one alignment file; returns the unified classes they point to."""
    ns = alignment["namespaces"]
    targets = set()
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
            if section == "classes":
                targets.add(target)
    for prefix, uri in ns.items():
        tele.bind(prefix, uri, replace=True)
    return targets


def main() -> None:
    cfg = load_yaml(PIPELINE)
    ns = cfg["namespaces"]
    unified = Graph()
    unified.parse(cfg["unified_ontology"])
    # coordinates are stored with WGS84 geo:lat / geo:long, next to the unifying ontology
    for prop in (GEO.lat, GEO.long):
        unified.add((prop, RDF.type, OWL.DatatypeProperty))

    tele = Graph()
    tele.add((URIRef("http://knowdive.disi.unitn.it/trentino-teleontology"), RDF.type, OWL.Ontology))
    for prop in (GEO.lat, GEO.long):
        tele.add((prop, RDF.type, OWL.DatatypeProperty))
    tele.bind("geo", GEO, replace=True)
    for prefix, uri in ns.items():
        tele.bind(prefix, uri, replace=True)

    for name, source in cfg["sources"].items():
        ontology = unified if source["ontology"] == cfg["unified_ontology"] else Graph().parse(source["ontology"])
        etypes = [expand(e, ns) for e in source["etypes"]]
        for e in etypes:
            check_exists(e, ontology, f"the {name} ontology")
        classes = with_ancestors(etypes, ontology)
        n_props = add_slice(tele, ontology, classes)
        msg = f"  {name}: {len(etypes)} e-types, {len(classes)} classes with ancestors, {n_props} properties"
        if source.get("alignment"):
            targets = add_alignment(tele, name, ontology, load_yaml(source["alignment"]), unified)
            add_slice(tele, unified, with_ancestors(targets, unified))
            msg += f", aligned to {len(targets)} unified classes"
        print(msg)

    out = cfg["outputs"]["teleontology"]
    tele.serialize(destination=ensure_parent(out), format="turtle", encoding="utf-8")
    print(f"Teleontology saved to {out} ({len(tele)} triples)")


if __name__ == "__main__":
    main()
