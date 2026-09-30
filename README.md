# Trentino KG Ingestion (iTelos approach)

Pipeline that integrates two knowledge graphs about Trentino into one unified KG:

* **OSM**: points of interest extracted from OpenStreetMap (2 km around the centre of Trento).
* **KGE**: the "Trentino tourist facilities" KG of the Knowledge Graph Engineering course
  (restaurants, hotels, museums, stops, ... and six tourists such as Germano Rossi).

Each source keeps its own ontology and its original URIs. The two ontologies are aligned
through a **teleontology** built on the unifying ontology (`OSM-GTFS-zzz.owl`, space and time),
and the entities that describe the same real-world thing are linked with `owl:sameAs`.

## Pipeline

| Stage | Script | Reads | Produces |
|---|---|---|---|
| 1 | `1_extraction.py` | `osm_source.yaml`, OpenStreetMap | `raw_osm_data.pkl` |
| 1 | `1_extraction_kge.py` | `kge_source.yaml`, `kge_data/*.csv` | `kge_kg.ttl` |
| 2 | `2_mapping.py` | `raw_osm_data.pkl`, `osm_source.yaml`, `alignment_*.yaml` | `osm_kg.ttl`, `teleontology.ttl` |
| 3 | `3_unification.py` | `unification.yaml` and the files above | `unified_kg.ttl`, `unification_report.csv` |

`run_pipeline.py` runs the stages in order and stops at the first error.
`python run_pipeline.py --skip-download` reuses `raw_osm_data.pkl` instead of querying OSM again.

### Stage 1: extraction
* **OSM**: downloads with osm2kg the elements with the tags listed in `osm_source.yaml`.
* **KGE**: the course repository has the final datasets as CSV but not the RDF graph (built
  with Karma), so the script rebuilds it from `kge_data/` with the classes and properties of
  the KGE ontology (`kge_ontology.owl`). Tourists are linked to their places by exact name.

### Stage 2: mapping
* **OSM source KG**: every element gets its original URI (`https://www.openstreetmap.org/node/<id>`,
  `way/<id>`, `relation/<id>`) and the class of the OSM ontology chosen by `osm_source.yaml`
  (e.g. `osm_ont:restaurant`). The OSM ontology is the unifying ontology, so these types need no
  further mapping. The centroid of the geometry is stored as `geo:lat` / `geo:long`.
* **Teleontology**: built from `alignment_kge.yaml`. Class operations: `subclass_of` (perfect
  correspondence, e.g. `etype:restaurant ⊑ osm_ont:restaurant`), `superclass_of` (the KGE class is
  broader, e.g. `osm_ont:bar, osm_ont:pub, osm_ont:cafe ⊑ etype:bar_pub`) and `extend` (concepts the
  unifying ontology does not have, e.g. `etype:tourist`). Property operations: `equivalent`
  (e.g. `etype:has_name ≡ osm_ont:name`) and `extend`.

### Stage 3: unification
Two entities are candidates only if their types are compatible in unifying ontology + teleontology
(one equal to or subclass of the other); this filter is always on. Then, as configured in
`unification.yaml`:
* **name**: normalized Levenshtein similarity ≥ `min_similarity`, after an optional normalization
  (accents, punctuation, generic words such as "hotel" or "ristorante");
* **coordinates**: distance ≤ `max_distance_m`.

Each primitive can be switched on or off; every enabled primitive must hold. With `one_to_one`
each entity keeps only its best candidate. The name and coordinate properties of each source are
found through the teleontology (all properties equivalent to `osm_ont:name`, `geo:lat`, `geo:long`).
`unification_report.csv` lists every link with its similarity and distance.

## Requirements
Python 3 with `osm2kg`, `pandas`, `geopandas`, `pyyaml`, `rdflib`. Optional: `rapidfuzz`
(faster Levenshtein, same results).

## GraphDB
Import `OSM-GTFS-zzz.owl`, `kge_ontology.owl`, `teleontology.ttl` and `unified_kg.ttl`.
Generated files are not versioned (see `.gitignore`).
