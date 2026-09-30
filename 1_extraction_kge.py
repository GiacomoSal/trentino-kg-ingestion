"""Stage 1 (KGE source): builds the KGE source KG from the course CSV files.

Every entity keeps the types and properties of the KGE ontology (kge_ontology.owl),
so the source KG is described only with its own ontology. Output: kge_kg.ttl
"""
import os
import sys

import pandas as pd
from rdflib import Graph, URIRef
from rdflib.namespace import RDF

from kg_utils import clean, expand, load_yaml, typed_literal

CONFIG = "kge_source.yaml"
OUTPUT = "kge_kg.ttl"


def read_csv(path: str) -> pd.DataFrame:
    df = pd.read_csv(path, dtype=str, encoding="utf-8")
    df.columns = [c.strip().lower() for c in df.columns]
    return df


def main() -> None:
    cfg = load_yaml(CONFIG)
    ns = cfg["namespaces"]
    base = cfg["base_uri"]
    csv_dir = cfg["csv_dir"]

    kg = Graph()
    for prefix, uri in ns.items():
        kg.bind(prefix, uri, replace=True)
    kg.bind("kge", base)

    # name -> entity URI, per class, used to resolve the tourists' relations
    by_name: dict = {}
    near = cfg.get("near")
    near_links = []

    for source in cfg["places"]:
        path = os.path.join(csv_dir, source["file"])
        if not os.path.exists(path):
            print(f"[ERROR] {path} not found")
            sys.exit(1)
        cls = expand(source["class"], ns)
        df = read_csv(path)
        for _, row in df.iterrows():
            entity_id = clean(row.get("id"))
            if entity_id is None:
                continue
            entity = URIRef(base + entity_id)
            kg.add((entity, RDF.type, cls))
            for column, spec in cfg["place_columns"].items():
                literal = typed_literal(row.get(column), spec["datatype"])
                if literal is not None:
                    kg.add((entity, expand(spec["property"], ns), literal))
            name = clean(row.get("name"))
            if name is not None:
                by_name.setdefault(cls, {})[name] = entity
            if near:
                for column in df.columns:
                    target_id = clean(row.get(column)) if column.startswith(near["column_prefix"]) else None
                    if target_id is not None:
                        near_links.append((entity, URIRef(base + target_id)))
        print(f"  {source['file']}: {len(df)} entities as {source['class']}")

    if near:
        known = set(kg.subjects(RDF.type, None))
        missing = [t for _, t in near_links if t not in known]
        for entity, target in near_links:
            if target in known:
                kg.add((entity, expand(near["property"], ns), target))
        print(f"  {len(near_links) - len(missing)} has_Near links"
              + (f", {len(missing)} pointing to unknown ids" if missing else ""))

    people = cfg["people"]
    df = read_csv(os.path.join(csv_dir, people["file"]))
    person_cls = expand(people["class"], ns)
    unresolved = []
    for _, row in df.iterrows():
        person = URIRef(base + row["id"])
        kg.add((person, RDF.type, person_cls))
        for column, spec in people["columns"].items():
            literal = typed_literal(row.get(column), spec["datatype"])
            if literal is not None:
                kg.add((person, expand(spec["property"], ns), literal))
        for column, spec in people["relations"].items():
            place_name = clean(row.get(column))
            if place_name is None:
                continue
            target = None
            for cls in spec["classes"]:
                target = by_name.get(expand(cls, ns), {}).get(place_name)
                if target is not None:
                    break
            if target is None:
                unresolved.append(f"{row['id']}.{column} = '{place_name}'")
                continue
            kg.add((person, expand(spec["property"], ns), target))
    print(f"  {people['file']}: {len(df)} tourists")
    for item in unresolved:
        print(f"  [WARNING] place not found in the KGE data: {item}")

    kg.serialize(destination=OUTPUT, format="turtle", encoding="utf-8")
    print(f"KGE source KG saved to {OUTPUT} ({len(kg)} triples)")


if __name__ == "__main__":
    main()
