# Trentino KG Ingestion (iTelos approach)

Pipeline that integrates knowledge graphs with different ontologies into one entity graph,
following the iTelos separation between teleontology, teleology and entity graph.

The case study uses two sources about Trentino:

* **OSM**: points of interest extracted from OpenStreetMap (2 km around the centre of Trento).
* **KGE**: the "Trentino tourist facilities" KG of the Knowledge Graph Engineering course
  (restaurants, hotels, museums, stops, ... and six tourists such as Germano Rossi).

To change or extend the pipeline, see the [developer guide](docs/DEVELOPER_GUIDE.md): what every
file does and which one to change for the most common changes.

## Repository structure

| Folder | Content |
|---|---|
| `config/` | `pipeline.yaml` (sources, unifying ontology, outputs) and the configuration of each stage |
| `ontologies/` | the unifying ontology `OSM-GTFS-zzz.owl` and the KGE ontology `kge_ontology.owl` |
| `datasets/kge/` | the CSV files of the KGE course |
| `src/` | the pipeline scripts |
| `cache/` | downloaded OSM data (not versioned) |
| `output/` | everything the pipeline generates (not versioned) |

## Outputs (in `output/`)

| File | What it is |
|---|---|
| `teleontology.ttl` | E-type graph: the e-types of the sources with their hierarchy and properties, plus their alignment to the unifying ontology (`ontologies/OSM-GTFS-zzz.owl`) |
| `teleology.ttl` | built from the teleontology: the e-types of the application that have instances, no hierarchy, each with all its properties; the entity graph is built on it |
| `entity_graph.ttl` | the entities of all sources, unchanged (original URI and type), plus the `owl:sameAs` links between the same real-world things |
| `unification_report.csv` | every link with its name similarity and distance |

## Pipeline

All the configuration is in `config/pipeline.yaml`. Extraction is source-specific; the other stages
are generic and only read `config/pipeline.yaml` and the files it points to.

| Stage | Script | Reads | Produces |
|---|---|---|---|
| 1 | `src/1_extraction_osm.py` | `config/osm_source.yaml`, OpenStreetMap | `osm_kg.ttl` (cache: `cache/raw_osm_data.pkl`) |
| 1 | `src/1_extraction_kge.py` | `config/kge_source.yaml`, `datasets/kge/*.csv` | `kge_kg.ttl` |
| 2 | `src/2_mapping.py` | `config/pipeline.yaml`, source ontologies, `config/alignment_*.yaml` | `teleontology.ttl` |
| 3 | `src/3_teleology.py` | `config/pipeline.yaml`, `teleontology.ttl`, source KGs | `teleology.ttl` |
| 4 | `src/4_unification.py` | `config/pipeline.yaml`, `config/unification.yaml`, source KGs, `teleology.ttl` | `entity_graph.ttl`, report |

From the repository root, `python run_pipeline.py` runs the extraction scripts listed in
`config/pipeline.yaml`, then stages 2-4, and stops at the first error.
`python run_pipeline.py --skip-download` reuses `cache/raw_osm_data.pkl`.

### Stage 1: extraction (source-specific)
* **OSM**: downloads with osm2kg the elements with the tags listed in `config/osm_source.yaml`. Every
  element keeps its original URI (`https://www.openstreetmap.org/node/<id>`, `way/<id>`,
  `relation/<id>`) and gets the class of the OSM ontology chosen by `config/osm_source.yaml`
  (e.g. `osm_ont:restaurant`). The centroid of the geometry is stored as `geo:lat` / `geo:long`.
* **KGE**: the course repository has the final datasets as CSV but not the RDF graph (built
  with Karma), so the script rebuilds it from `datasets/kge/` with the classes and properties of
  the KGE ontology (`ontologies/kge_ontology.owl`). Tourists are linked to their places by exact name.

### Stage 2: mapping
Builds the teleontology from the sources in `config/pipeline.yaml`. For each source it takes, from
its ontology, the e-types the application uses (`etypes`), their ancestors and the properties whose
domain is one of them, and adds the alignment file, which connects them to the unifying ontology
with two operations, for classes and properties alike: `equivalent` (e.g. `etype:has_name ≡
osm_ont:name`) and `child_of` (e.g. `etype:restaurant ⊑ osm_ont:restaurant`). Terms with no rule
(e.g. `etype:tourist`) are not mapped. Sources described with the unifying ontology itself (OSM)
need no alignment.

### Stage 3: teleology
Built from the teleontology: the minimal subset needed by the input data, i.e. the e-types listed
in `config/pipeline.yaml` that have at least one instance in their source KG (the others are
reported and left out), with no hierarchy. Each
e-type gets the properties of its class and of its ancestors in its own source hierarchy, so they are
collapsed onto it. The alignment is not followed here: it stays in the teleontology. Properties are
attached with `schema:domainIncludes` / `schema:rangeIncludes`, which, unlike `rdfs:domain` /
`rdfs:range`, do not make a reasoner infer new types.

### Stage 4: unification
The entity graph is built on the teleology: the script checks that every entity type is one of its
e-types. For every pair of sources listed under `links` in `config/unification.yaml`, two entities are candidates
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
An alternative configuration can be passed on the command line: `python src/4_unification.py other.yaml`.

## Adding a source
1. Get its KG in RDF: an existing file, or a new extraction script listed under `extraction`.
2. If it is not described with the unifying ontology, write `config/alignment_<name>.yaml`.
3. Add it under `sources` in `config/pipeline.yaml` (KG, ontology, e-types used, alignment) and
   the pair to link under `links` in `config/unification.yaml`.

No change to the generic stages is needed.

## Requirements
Python 3 with the packages in `requirements.txt`, plus `osm2kg` installed from its local folder:

    python3 -m venv venv
    source venv/bin/activate
    pip install -r requirements.txt
    pip install -e /path/to/osm2kg-main

## GraphDB
* Application view: `output/teleology.ttl` + `output/entity_graph.ttl`.
* Schema view: `ontologies/OSM-GTFS-zzz.owl` + `output/teleontology.ttl`.

The teleontology can also be loaded in the same repository as the entity graph: the e-types have
the same URIs, so the hierarchy and the alignment attach to the entities automatically.
