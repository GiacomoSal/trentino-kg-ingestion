"""Stage 4: teleology (generic).

Builds the teleology of the application from the entity graph: the flat schema made of
only the e-types the entities actually use, with no hierarchy. Each e-type lists all the
properties its entities use, inherited ones included, so data and object properties are
"collapsed" onto the e-type itself (iTelos). The hierarchy and the alignment stay in the
teleontology, which is a separate file.

Properties are attached to e-types with schema:domainIncludes / schema:rangeIncludes:
unlike rdfs:domain / rdfs:range they do not make a reasoner infer new types, so a
property shared by several e-types (e.g. a name) does not merge them.
"""
from collections import defaultdict

from rdflib import Graph, Literal, Namespace, URIRef
from rdflib.namespace import OWL, RDF, RDFS, XSD

from kg_utils import ensure_parent, load_yaml

PIPELINE = "config/pipeline.yaml"
SCHEMA = Namespace("http://schema.org/")
SKIP_PREDICATES = {RDF.type, OWL.sameAs}
SKIP_NAMESPACES = (str(RDF), str(RDFS), str(OWL))


def local_name(uri: URIRef) -> str:
    text = str(uri)
    for sep in ("#", "/"):
        if sep in text:
            text = text.rsplit(sep, 1)[1]
    return text


def main() -> None:
    cfg = load_yaml(PIPELINE)
    out = cfg["outputs"]
    eg = Graph()
    eg.parse(out["entity_graph"])
    # labels and comments come from the schemas the e-types are defined in
    schema = Graph()
    schema.parse(cfg["unified_ontology"])
    schema.parse(out["teleontology"])

    types_of = defaultdict(set)
    for s, t in eg.subject_objects(RDF.type):
        if isinstance(t, URIRef) and not str(t).startswith(SKIP_NAMESPACES):
            types_of[s].add(t)

    domains = defaultdict(set)      # property -> e-types whose entities use it
    ranges = defaultdict(set)       # property -> datatypes or e-types of the values
    is_object = defaultdict(bool)
    for s, types in types_of.items():
        for p, o in eg.predicate_objects(s):
            if p in SKIP_PREDICATES:
                continue
            domains[p] |= types
            if isinstance(o, Literal):
                ranges[p].add(o.datatype or XSD.string)
            else:
                is_object[p] = True
                ranges[p] |= types_of.get(o, set())

    tele = Graph()
    for prefix, uri in eg.namespaces():
        tele.bind(prefix, uri, replace=True)
    tele.bind("schema", SCHEMA, override=True, replace=True)
    tele.add((URIRef("http://knowdive.disi.unitn.it/trentino-teleology"), RDF.type, OWL.Ontology))

    etypes = set().union(*types_of.values())
    for cls in etypes:
        tele.add((cls, RDF.type, OWL.Class))
        labels = list(schema.objects(cls, RDFS.label)) or [Literal(local_name(cls))]
        for label in labels:
            tele.add((cls, RDFS.label, label))
        for comment in schema.objects(cls, RDFS.comment):
            tele.add((cls, RDFS.comment, comment))

    for p in domains:
        tele.add((p, RDF.type, OWL.ObjectProperty if is_object[p] else OWL.DatatypeProperty))
        for label in list(schema.objects(p, RDFS.label)) or [Literal(local_name(p))]:
            tele.add((p, RDFS.label, label))
        for cls in domains[p]:
            tele.add((p, SCHEMA.domainIncludes, cls))
        for r in ranges[p]:
            tele.add((p, SCHEMA.rangeIncludes, r))

    tele.serialize(destination=ensure_parent(out["teleology"]), format="turtle", encoding="utf-8")
    print(f"Teleology saved to {out['teleology']}: {len(etypes)} e-types, {len(domains)} properties, "
          f"no hierarchy ({len(tele)} triples)")


if __name__ == "__main__":
    main()
