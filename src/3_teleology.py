"""Stage 3: teleology (generic), built from the teleontology.

The teleology is the flat schema the entity graph is built on: only the e-types the
application uses (listed for each source in config/pipeline.yaml), with no hierarchy.
Each e-type gets all the properties of its own class and of its ancestors in the
source hierarchy, so data and object properties are "collapsed" onto it (iTelos).
The alignment to the unifying ontology is not followed here: it stays in the
teleontology, and each source keeps its own e-types and properties.

Properties are attached with schema:domainIncludes / schema:rangeIncludes: unlike
rdfs:domain / rdfs:range they do not make a reasoner infer new types, so a property
shared by several e-types (e.g. a name) does not merge them.
"""
from collections import defaultdict
from typing import Set

from rdflib import Graph, Literal, Namespace, URIRef
from rdflib.namespace import OWL, RDF, RDFS

from kg_utils import ensure_parent, expand, load_yaml

PIPELINE = "config/pipeline.yaml"
SCHEMA = Namespace("http://schema.org/")


def namespace(uri: URIRef) -> str:
    text = str(uri)
    return text.rsplit("#", 1)[0] if "#" in text else text.rsplit("/", 1)[0]


def own_ancestors(cls: URIRef, tele: Graph) -> Set[URIRef]:
    """The class and its superclasses in its own source hierarchy.

    Alignment axioms link classes of different sources (different namespaces), so
    following only same-namespace subclass links stays inside the source hierarchy.
    """
    ns = namespace(cls)
    seen, todo = {cls}, [cls]
    while todo:
        for parent in tele.objects(todo.pop(), RDFS.subClassOf):
            if isinstance(parent, URIRef) and namespace(parent) == ns and parent not in seen:
                seen.add(parent)
                todo.append(parent)
    return seen


def local_name(uri: URIRef) -> str:
    text = str(uri)
    for sep in ("#", "/"):
        if sep in text:
            text = text.rsplit(sep, 1)[1]
    return text


def main() -> None:
    cfg = load_yaml(PIPELINE)
    ns = cfg["namespaces"]
    out = cfg["outputs"]
    tele = Graph()
    tele.parse(out["teleontology"])

    # property -> e-types it is collapsed onto
    domains = defaultdict(set)
    etypes = []
    for source in cfg["sources"].values():
        for e in source["etypes"]:
            etype = expand(e, ns)
            etypes.append(etype)
            ancestors = own_ancestors(etype, tele)
            for prop, domain in tele.subject_objects(RDFS.domain):
                if domain in ancestors:
                    domains[prop].add(etype)

    teleology = Graph()
    for prefix, uri in tele.namespaces():
        teleology.bind(prefix, uri, replace=True)
    teleology.bind("schema", SCHEMA, override=True, replace=True)
    teleology.add((URIRef("http://knowdive.disi.unitn.it/trentino-teleology"), RDF.type, OWL.Ontology))

    for etype in etypes:
        teleology.add((etype, RDF.type, OWL.Class))
        for label in list(tele.objects(etype, RDFS.label)) or [Literal(local_name(etype))]:
            teleology.add((etype, RDFS.label, label))
        for comment in tele.objects(etype, RDFS.comment):
            teleology.add((etype, RDFS.comment, comment))

    for prop, classes in domains.items():
        for kind in (OWL.ObjectProperty, OWL.DatatypeProperty):
            if (prop, RDF.type, kind) in tele:
                teleology.add((prop, RDF.type, kind))
        for label in list(tele.objects(prop, RDFS.label)) or [Literal(local_name(prop))]:
            teleology.add((prop, RDFS.label, label))
        for cls in classes:
            teleology.add((prop, SCHEMA.domainIncludes, cls))
        for r in tele.objects(prop, RDFS.range):
            teleology.add((prop, SCHEMA.rangeIncludes, r))

    teleology.serialize(destination=ensure_parent(out["teleology"]), format="turtle", encoding="utf-8")
    print(f"Teleology saved to {out['teleology']}: {len(etypes)} e-types, {len(domains)} properties, "
          f"no hierarchy ({len(teleology)} triples)")


if __name__ == "__main__":
    main()
