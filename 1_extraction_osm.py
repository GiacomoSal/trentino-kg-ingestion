"""Stage 1 (OSM source): builds the OSM source KG (osm_kg.ttl).

Source-specific. Downloads with osm2kg the elements whose tags appear in
osm_source.yaml (cached in raw_osm_data.pkl), then turns each element into an
entity that keeps its original OpenStreetMap URI and gets the class of the OSM
ontology chosen by osm_source.yaml. The OSM ontology is also the unifying
ontology, so these types need no further alignment.

Use --skip-download to reuse raw_osm_data.pkl instead of querying OSM again.
"""
import os
import sys
from typing import Optional, Tuple

import pandas as pd
from rdflib import Graph, Literal, Namespace, URIRef
from rdflib.namespace import RDF, XSD

from kg_utils import clean, expand, load_yaml

CONFIG = "osm_source.yaml"
RAW_DATA = "raw_osm_data.pkl"
OUTPUT = "osm_kg.ttl"

# Trento city centre and extraction radius
CENTER = (46.0678, 11.1211)
RADIUS_M = 2000

OSM_ELEMENT = "https://www.openstreetmap.org/"
OSM_KEY = Namespace("https://wiki.openstreetmap.org/wiki/Key:")
GEO = Namespace("http://www.w3.org/2003/01/geo/wgs84_pos#")


# ---------------------------------------------------------------- download

def tags_to_download(cfg: dict) -> dict:
    """Aggregates the tag filters of all rules into one dictionary for osm2kg."""
    tags: dict = {}
    for rule in cfg.get("mappings", []):
        for key, values in rule.get("osm_filter", {}).items():
            tags.setdefault(key, [])
            for v in values:
                if v not in tags[key]:
                    tags[key].append(v)
    return tags


def download(cfg: dict) -> None:
    import osm2kg as og  # only needed when downloading

    tags = tags_to_download(cfg)
    print(f"Downloading OSM data via osm2kg, tags: {tags}")
    gdf = og.feature.features_from_point(CENTER, tags=tags, dist=RADIUS_M)
    # keep only the elements with a name
    gdf = og.feature.filter_gdf(gdf, including_filters={"name": True})
    gdf.to_pickle(RAW_DATA)
    print(f"Raw data cached in {RAW_DATA}")


# ---------------------------------------------------------------- source KG

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

        # point used by the geographic primitive: the centroid of the geometry
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


def main() -> None:
    cfg = load_yaml(CONFIG)
    if "--skip-download" in sys.argv:
        if not os.path.exists(RAW_DATA):
            print(f"[ERROR] --skip-download needs {RAW_DATA}")
            sys.exit(1)
        print(f"Using cached {RAW_DATA}")
    else:
        download(cfg)

    raw = pd.read_pickle(RAW_DATA)
    kg = build_osm_kg(raw, cfg)
    kg.serialize(destination=OUTPUT, format="turtle", encoding="utf-8")
    print(f"OSM source KG saved to {OUTPUT} ({len(kg)} triples)")


if __name__ == "__main__":
    main()
