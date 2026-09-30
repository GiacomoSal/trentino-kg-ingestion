"""Stage 2: mapping.

a) Builds the OSM source KG (osm_kg.ttl) from the data extracted in stage 1. Each
   element keeps its original OpenStreetMap URI and gets the class of the OSM
   ontology chosen by osm_source.yaml. The OSM ontology is also the unifying
   ontology, so these types need no further mapping.
b) Builds the teleontology (teleontology.ttl) from the alignment files
   (alignment_*.yaml): the axioms that connect the types and properties of the
   other sources to the unifying ontology, plus the concepts they add to it.
   The entities keep their original types: the alignment lives only here.
"""
import glob
import sys
from typing import Iterable, Optional, Tuple

import pandas as pd
from rdflib import Graph, Literal, Namespace, URIRef
from rdflib.namespace import OWL, RDF, RDFS, XSD

from kg_utils import clean, expand, load_yaml

OSM_CONFIG = "osm_source.yaml"
RAW_DATA = "raw_osm_data.pkl"
OSM_OUTPUT = "osm_kg.ttl"
UNIFIED_ONTOLOGY = "OSM-GTFS-zzz.owl"
TELEONTOLOGY_OUTPUT = "teleontology.ttl"

OSM_ELEMENT = "https://www.openstreetmap.org/"
OSM_KEY = Namespace("https://wiki.openstreetmap.org/wiki/Key:")
GEO = Namespace("http://www.w3.org/2003/01/geo/wgs84_pos#")


# ---------------------------------------------------------------- a) OSM source KG

def element_ref(index_value, row) -> Optional[Tuple[str, str]]:
    """Returns (element type, numeric id) of an OSM element.

    osm2kg indexes the GeoDataFrame by (element, id), e.g. ('node', 1147756663).
    The element type is part of the URI: node, way and relation ids can coincide.
    """
    if isinstance(index_value, tuple) and len(index_value) == 2:
        element, osm_id = index_value
    else:
        element, osm_id = row.get("element"), row.get("id", index_value)
    element, osm_id = clean(element), clean(osm_id)
    if element is None or osm_id is None:
        return None
    return str(element), str(int(osm_id))


def match_rule(tags, rules, ns) -> Optional[URIRef]:
    """Class of the first rule whose filter matches the element's tags."""
    for rule in rules:
        for key, allowed in rule.get("osm_filter", {}).items():
            if clean(tags.get(key)) in allowed:
                return expand(rule["target_class"], ns)
    return None


def build_osm_kg(raw: pd.DataFrame, cfg: dict) -> Graph:
    ns = cfg["namespaces"]
    osm_ont = Namespace(ns["osm_ont"])
    kg = Graph()
    kg.bind("osm_ont", osm_ont)
    kg.bind("geo", GEO, replace=True)
    kg.bind("osmkey", OSM_KEY)

    skipped = 0
    for index_value, row in raw.iterrows():
        ref = element_ref(index_value, row)
        cls = match_rule(row, cfg["mappings"], ns)
        if ref is None or cls is None:
            skipped += 1
            continue
        element, osm_id = ref
        node = URIRef(f"{OSM_ELEMENT}{element}/{osm_id}")

        kg.add((node, RDF.type, cls))
        kg.add((node, osm_ont.osm_id, Literal(int(osm_id), datatype=XSD.integer)))
        name = clean(row.get("name"))
        if isinstance(name, str):
            kg.add((node, osm_ont.name, Literal(name, datatype=XSD.string)))

        # Point used for the geographic primitive: the centroid of the geometry
        geometry = row.get("geometry")
        if geometry is not None and not getattr(geometry, "is_empty", False):
            centroid = geometry.centroid
            kg.add((node, GEO.lat, Literal(round(centroid.y, 7), datatype=XSD.float)))
            kg.add((node, GEO.long, Literal(round(centroid.x, 7), datatype=XSD.float)))

        for tag in cfg.get("tags_as_properties", []):
            value = clean(row.get(tag))
            if value is not None:
                # ':' is percent-encoded so the key stays one local name (osmkey:addr%3Astreet)
                kg.add((node, OSM_KEY[tag.replace(":", "%3A")], Literal(str(value), datatype=XSD.string)))

    if skipped:
        print(f"  {skipped} elements skipped (no id or no matching rule)")
    return kg


# ---------------------------------------------------------------- b) teleontology

def check_exists(term: URIRef, ontology: Graph, where: str) -> None:
    if (term, None, None) not in ontology:
        print(f"  [WARNING] {term} is not defined in {where}")


def copy_declaration(term: URIRef, source: Graph, target: Graph, predicates: Iterable) -> None:
    for predicate in predicates:
        for value in source.objects(term, predicate):
            target.add((term, predicate, value))


def add_alignment(tele: Graph, alignment: dict, unified: Graph) -> None:
    ns = alignment["namespaces"]
    source = Graph()
    source.parse(alignment["source_ontology"])
    name = alignment["source"]

    for rule in alignment.get("classes", []):
        cls = expand(rule["class"], ns)
        check_exists(cls, source, f"the {name} ontology")
        tele.add((cls, RDF.type, OWL.Class))
        copy_declaration(cls, source, tele, [RDFS.label, RDFS.comment])
        op = rule["operation"]
        if op == "subclass_of":
            target = expand(rule["target"], ns)
            check_exists(target, unified, "the unifying ontology")
            tele.add((cls, RDFS.subClassOf, target))
        elif op == "superclass_of":
            for t in rule["targets"]:
                target = expand(t, ns)
                check_exists(target, unified, "the unifying ontology")
                tele.add((target, RDFS.subClassOf, cls))
            if rule.get("parent"):
                tele.add((cls, RDFS.subClassOf, expand(rule["parent"], ns)))
        elif op == "extend":
            if rule.get("parent"):
                parent = expand(rule["parent"], ns)
                check_exists(parent, unified, "the unifying ontology")
                tele.add((cls, RDFS.subClassOf, parent))
        else:
            print(f"  [ERROR] unknown class operation '{op}' for {rule['class']}")
            sys.exit(1)

    for rule in alignment.get("properties", []):
        prop = expand(rule["property"], ns)
        check_exists(prop, source, f"the {name} ontology")
        for kind in (OWL.ObjectProperty, OWL.DatatypeProperty):
            if (prop, RDF.type, kind) in source:
                tele.add((prop, RDF.type, kind))
        op = rule["operation"]
        if op == "equivalent":
            tele.add((prop, OWL.equivalentProperty, expand(rule["target"], ns)))
        elif op == "extend":
            copy_declaration(prop, source, tele, [RDFS.domain, RDFS.range, RDFS.label])
        else:
            print(f"  [ERROR] unknown property operation '{op}' for {rule['property']}")
            sys.exit(1)

    for prefix, uri in ns.items():
        tele.bind(prefix, uri, replace=True)


def build_teleontology() -> Graph:
    unified = Graph()
    unified.parse(UNIFIED_ONTOLOGY)
    tele = Graph()
    tele.add((URIRef("http://knowdive.disi.unitn.it/trentino-teleontology"), RDF.type, OWL.Ontology))
    files = sorted(glob.glob("alignment_*.yaml"))
    for path in files:
        print(f"  alignment: {path}")
        add_alignment(tele, load_yaml(path), unified)
    # the coordinate properties used by the OSM source KG
    tele.add((GEO.lat, RDF.type, OWL.DatatypeProperty))
    tele.add((GEO.long, RDF.type, OWL.DatatypeProperty))
    tele.bind("geo", GEO, replace=True)
    return tele


def main() -> None:
    cfg = load_yaml(OSM_CONFIG)
    try:
        raw = pd.read_pickle(RAW_DATA)
    except FileNotFoundError:
        print(f"[ERROR] {RAW_DATA} not found. Run 1_extraction.py first.")
        sys.exit(1)

    print("Building the OSM source KG...")
    osm_kg = build_osm_kg(raw, cfg)
    osm_kg.serialize(destination=OSM_OUTPUT, format="turtle", encoding="utf-8")
    print(f"OSM source KG saved to {OSM_OUTPUT} ({len(osm_kg)} triples)")

    print("Building the teleontology...")
    tele = build_teleontology()
    tele.serialize(destination=TELEONTOLOGY_OUTPUT, format="turtle", encoding="utf-8")
    print(f"Teleontology saved to {TELEONTOLOGY_OUTPUT} ({len(tele)} triples)")


if __name__ == "__main__":
    main()
