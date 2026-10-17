# Trentino KG Ingestion (iTelos approach)

Pipeline that integrates knowledge graphs with different ontologies into one entity graph,
following the iTelos separation between teleontology, teleology and entity graph.

The case study uses two sources about Trentino:

* **OSM**: points of interest extracted from OpenStreetMap (2 km around the centre of Trento).
* **KGE**: the "Trentino tourist facilities" KG of the Knowledge Graph Engineering course
  (restaurants, hotels, museums, stops, ... and six tourists such as Germano Rossi).

## Outputs

| File | What it is |
|---|---|
| `teleontology.ttl` | E-type graph: classes, hierarchy and properties of the sources, plus their alignment to the unifying ontology (`OSM-GTFS-zzz.owl`) |
| `entity_graph.ttl` | the entities of all sources, unchanged (original URI and type), plus the `owl:sameAs` links between the same real-world things |
| `teleology.ttl` | flat schema of the entity graph: only the e-types it uses, no hierarchy, each with all its properties |
| `unification_report.csv` | every link with its name similarity and distance |

## Pipeline

All the configuration is in `pipeline.yaml`. Extraction is source-specific; the other stages
are generic and only read `pipeline.yaml` and the files it points to.

| Stage | Script | Reads | Produces |
|---|---|---|---|
| 1 | `1_extraction_osm.py` | `osm_source.yaml`, OpenStreetMap | `osm_kg.ttl` (cache: `raw_osm_data.pkl`) |
| 1 | `1_extraction_kge.py` | `kge_source.yaml`, `kge_data/*.csv` | `kge_kg.ttl` |
| 2 | `2_mapping.py` | `pipeline.yaml`, source ontologies, `alignment_*.yaml` | `teleontology.ttl` |
| 3 | `3_unification.py` | `pipeline.yaml`, `unification.yaml`, source KGs | `entity_graph.ttl`, report |
| 4 | `4_teleology.py` | `entity_graph.ttl` | `teleology.ttl` |

`run_pipeline.py` runs the extraction scripts listed in `pipeline.yaml`, then stages 2-4, and
stops at the first error. `python run_pipeline.py --skip-download` reuses `raw_osm_data.pkl`.

### Stage 1: extraction (source-specific)
* **OSM**: downloads with osm2kg the elements with the tags listed in `osm_source.yaml`. Every
  element keeps its original URI (`https://www.openstreetmap.org/node/<id>`, `way/<id>`,
  `relation/<id>`) and gets the class of the OSM ontology chosen by `osm_source.yaml`
  (e.g. `osm_ont:restaurant`). The centroid of the geometry is stored as `geo:lat` / `geo:long`.
* **KGE**: the course repository has the final datasets as CSV but not the RDF graph (built
  with Karma), so the script rebuilds it from `kge_data/` with the classes and properties of
  the KGE ontology (`kge_ontology.owl`). Tourists are linked to their places by exact name.

### Stage 2: mapping
Builds the teleontology from the sources in `pipeline.yaml`: the schema of each source ontology
plus its alignment file, which connects it to the unifying ontology with two operations, for
classes and properties alike: `equivalent` (e.g. `etype:has_name ≡ osm_ont:name`) and `child_of`
(e.g. `etype:restaurant ⊑ osm_ont:restaurant`). Terms with no rule (e.g. `etype:tourist`) are not
mapped. Sources already described with the unifying ontology (OSM) need no alignment.

### Stage 3: unification
For every pair of sources listed under `links` in `unification.yaml`, two entities are candidates
only if their types are compatible in unifying ontology + teleontology (one equal to or subclass
of the other); this filter is always on. Then:
* **name**: Levenshtein similarity ≥ `min_similarity`, after an optional normalization (accents,
  punctuation, generic words such as "hotel" or "ristorante"). With `method: levenshtein` the whole
  names are compared; with `method: partial` the shorter name is compared with the most similar part
  of the longer one, so "FORST" matches "Forsterbräu Trento";
* **coordinates**: distance ≤ `max_distance_m`.

Each primitive can be switched on or off; every enabled primitive must hold. With `one_to_one`
each entity keeps only its best candidate. The name and coordinate properties of each source are
found through the teleontology (all properties equivalent to `osm_ont:name`, `geo:lat`, `geo:long`).
The script also prints the collisions, i.e. the entities that had more than one candidate.
An alternative configuration can be passed on the command line: `python 3_unification.py other.yaml`.

### Stage 4: teleology
Reads the entity graph and writes the e-types its entities use, with no hierarchy. Each e-type lists
all the properties its entities use, inherited ones included. Properties are attached with
`schema:domainIncludes` / `schema:rangeIncludes`, which, unlike `rdfs:domain` / `rdfs:range`, do not
make a reasoner infer new types.

## Adding a source
1. Get its KG in RDF: an existing file, or a new extraction script listed under `extraction`.
2. If it is not described with the unifying ontology, write `alignment_<name>.yaml`.
3. Add it under `sources` in `pipeline.yaml` and the pair to link under `links` in `unification.yaml`.

No change to the generic stages is needed.

## Requirements
Python 3 with the packages in `requirements.txt`, plus `osm2kg` installed from its local folder:

    python3 -m venv venv
    source venv/bin/activate
    pip install -r requirements.txt
    pip install -e /path/to/osm2kg-main

## GraphDB
* Application view: `teleology.ttl` + `entity_graph.ttl`.
* Schema view: `OSM-GTFS-zzz.owl` + `teleontology.ttl`.

Generated files are not versioned (see `.gitignore`).
