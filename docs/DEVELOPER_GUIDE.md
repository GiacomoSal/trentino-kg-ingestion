# Developer guide

This guide is for whoever takes over the pipeline. It explains what every file does, how
the files depend on each other and which file to change for the most common changes.
For a short overview and the installation steps, see the [README](../README.md).

## 1. The idea in one page

The pipeline takes several knowledge graphs (KGs), each described with its own ontology,
and produces a single entity graph in which the entities that describe the same real-world
thing are linked with `owl:sameAs`. It follows the iTelos separation into three layers:

| iTelos layer | File produced | Built by |
|---|---|---|
| Teleontology (E-type graph: e-types, hierarchy, properties, alignment to other schemas) | `output/teleontology.ttl` | `src/2_mapping.py` |
| Teleology (flat schema of the application: e-types without hierarchy, each with all its properties) | `output/teleology.ttl` | `src/3_teleology.py` |
| Entity graph (the entities, built on the teleology) | `output/entity_graph.ttl` | `src/4_unification.py` |

Three rules hold everywhere and should be kept when changing the code:

1. **Sources are never modified.** Every entity keeps its original URI, type and properties.
   Entities are never merged: two entities of different sources that describe the same
   thing are only linked with `owl:sameAs`.
2. **Only stage 1 knows about the sources.** Stages 2-4 are generic: they only read
   `config/pipeline.yaml` and the files it points to. Adding a source must not require
   changes to stages 2-4.
3. **Alignment uses two operations only**: `equivalent` and `child_of`, for classes and
   properties alike. Each source keeps its own e-types; the alignment connects them to the
   unifying ontology but does not replace them.

## 2. Data flow

```
                    config/osm_source.yaml             config/kge_source.yaml
                            |                                   |
 OpenStreetMap --> src/1_extraction_osm.py         datasets/kge/*.csv --> src/1_extraction_kge.py
   (cache/raw_osm_data.pkl) |                                   |
                            v                                   v
                    output/osm_kg.ttl                   output/kge_kg.ttl
                            \                                  /
 ontologies/*.owl            \                                /
 config/alignment_*.yaml --> src/2_mapping.py --> output/teleontology.ttl
 config/pipeline.yaml                                  |
                                                       v
                                  src/3_teleology.py --> output/teleology.ttl
                                                       |
 config/unification.yaml ---------------------------->  v
 source KGs + teleontology + teleology --> src/4_unification.py --> output/entity_graph.ttl
                                                                    output/unification_report.csv
```

`run_pipeline.py` runs everything in this order and stops at the first error.

## 3. Repository map

### Root

| File | What it does |
|---|---|
| `run_pipeline.py` | Runs the extraction scripts listed under `extraction` in `config/pipeline.yaml`, then stages 2, 3 and 4. Passes `--skip-download` to the extraction scripts if given. |
| `requirements.txt` | Python packages. `osm2kg` is not on PyPI and is installed separately from its local folder (see README). |
| `README.md` | Overview, installation, GraphDB views. |

All scripts must be run **from the repository root**: paths in the code and in the
configuration files are relative to it.

### `config/`

| File | Read by | What it contains |
|---|---|---|
| `pipeline.yaml` | stages 2, 3, 4 and `run_pipeline.py` | The unifying ontology, the extraction scripts, and for every source: its KG, its ontology, the e-types the application uses and its alignment file. Also the output paths. **This is the central file.** |
| `osm_source.yaml` | `1_extraction_osm.py` | Which OSM tags to download, which OSM ontology class each element gets (first matching rule wins), which OSM tags are copied as properties. |
| `kge_source.yaml` | `1_extraction_kge.py` | Which CSV file becomes which KGE class, which column becomes which property, the `has_Near` columns, and how the tourists are linked to their places. |
| `alignment_kge.yaml` | `2_mapping.py` | Alignment of the KGE ontology to the unifying ontology (`equivalent` / `child_of` rules for classes and properties). |
| `unification.yaml` | `4_unification.py` | Which pairs of sources to link and the settings of the unification primitives (name, coordinates, one-to-one). |

### `ontologies/`

| File | What it is |
|---|---|
| `OSM-GTFS-zzz.owl` | The unifying ontology (space and time), from the previous work of Cornella. Only the OSM part is used. OSM entities are typed directly with its classes. |
| `kge_ontology.owl` | The ontology of the KGE course project (`etype:` namespace). |

The odd namespaces inside the OWL files (`.../lixiaoyue/ontologies/2023/2/untitled-ontology-26#`,
`.../hamid/ontologies/2022/11/...`) are the default IRIs that Protégé assigns to a new
ontology. They are just identifiers; do not change them, or the classes used in the
configuration files will no longer match.

### `datasets/kge/`

The final CSV files of the KGE course project ("Trentino tourist facilities"): one file per
kind of place, plus `personas.csv` with the six tourists.

### `src/`

| File | Stage | Generic? | What it does |
|---|---|---|---|
| `1_extraction_osm.py` | 1 | no (OSM only) | Downloads the OSM elements with `osm2kg` (or reads the cache) and writes `output/osm_kg.ttl`. |
| `1_extraction_kge.py` | 1 | no (KGE only) | Rebuilds the KGE KG from the CSV files and writes `output/kge_kg.ttl`. |
| `2_mapping.py` | 2 | yes | Builds the teleontology. |
| `3_teleology.py` | 3 | yes | Builds the teleology from the teleontology. |
| `4_unification.py` | 4 | yes | Links the entities and writes the entity graph and the report. |
| `kg_utils.py` | - | yes | Small helpers shared by all scripts. |

### `cache/` and `output/`

Both are generated and not versioned (see `.gitignore`). Deleting them is safe:
`output/` is rebuilt by the pipeline, `cache/` by a run without `--skip-download`.

## 4. The scripts in detail

### `src/kg_utils.py`

- `load_yaml(path)`: reads a configuration file.
- `expand(curie, namespaces)`: turns `prefix:local` into a full URI using the `namespaces`
  block of a configuration file. Every configuration file has its own `namespaces` block,
  so a prefix must be declared in the file that uses it.
- `clean(value)`: returns `None` for missing values (None, NaN, empty strings).
- `typed_literal(value, datatype)`: builds an RDF literal (`string`, `float`, `int`,
  `integer`), or `None` if the value is missing or invalid.
- `ensure_parent(path)`: creates the folder of a file before writing it.

### `src/1_extraction_osm.py` (source-specific)

- `CENTER` and `RADIUS_M` (top of the file): the area downloaded, currently 2 km around the
  centre of Trento.
- `download()`: calls `osm2kg` with the tags of all the rules in `osm_source.yaml`, keeps only
  the elements with a name and caches them in `cache/raw_osm_data.pkl`.
- `build_osm_kg()`: for every element:
  - URI: `https://www.openstreetmap.org/<node|way|relation>/<id>` (the element type is part
    of the URI because node, way and relation ids can coincide);
  - type: the class of the first rule in `osm_source.yaml` whose filter matches;
  - `osm_ont:osm_id`, `osm_ont:name`;
  - `geo:lat` / `geo:long`: the centroid of the geometry (for polygons this can be far from
    the entrance of a large building);
  - the tags in `tags_as_properties`, as `osmkey:<tag>` properties (`:` is encoded as `%3A`).
- `--skip-download` reuses the cache instead of querying OpenStreetMap.

### `src/1_extraction_kge.py` (source-specific)

The course repository contains the CSV files and the ontology but not the RDF graph (it was
built with Karma and never published), so this script rebuilds it:

- every row of every file in `places` becomes an entity with URI `base_uri + id` and the class
  of that file;
- the columns in `place_columns` become data properties;
- the columns starting with `n_` hold the id of the nearest entity of another dataset and
  become `etype:has_Near` links;
- every row of `personas.csv` becomes an `etype:tourist`; the places named in its columns are
  looked up **by exact name** among the classes listed in `relations` and linked with the
  given object property. Places that are not found are printed as warnings (currently
  `Monte Bondone`).

### `src/2_mapping.py` (generic): teleontology

For every source in `pipeline.yaml`:

1. takes the `etypes` of the source and all their ancestors in the source ontology
   (`with_ancestors`);
2. copies those classes (labels, comments, subclass links among them) and every data or object
   property whose `rdfs:domain` is one of them (`add_slice`);
3. if the source has an `alignment` file, adds its rules as axioms (`add_alignment`):

   | operation | classes | properties |
   |---|---|---|
   | `equivalent` | `owl:equivalentClass` | `owl:equivalentProperty` |
   | `child_of` | `rdfs:subClassOf` | `rdfs:subPropertyOf` |

   and copies the unified classes the rules point to, with their ancestors.

A source whose `ontology` is the unifying ontology itself (OSM) needs no alignment.
The script prints a warning for every term that does not exist in the ontology it should come
from, and stops on an unknown operation.

### `src/3_teleology.py` (generic): teleology

Reads the teleontology (not the entity graph) and writes, for every e-type listed in
`pipeline.yaml`:

- the e-type as an `owl:Class`, with its label and comment, **without any hierarchy**;
- every property whose domain is the e-type or one of its ancestors, attached with
  `schema:domainIncludes` (and `schema:rangeIncludes` for the range). This is the
  "collapsing" of properties onto the e-type.

Two details matter:

- `own_ancestors()` follows only subclass links **inside the namespace of the e-type**.
  Alignment axioms always point to another namespace (the unifying ontology), so they are not
  followed: each source keeps its own e-types and properties. A new source must therefore use
  a namespace different from the unifying ontology, unless (like OSM) it is described with the
  unifying ontology itself.
- `schema:domainIncludes` is used instead of `rdfs:domain` on purpose: `rdfs:domain` would make
  a reasoner infer new types (a property shared by many e-types would give every entity all of
  them).

### `src/4_unification.py` (generic): entity graph

1. Loads the unifying ontology plus the teleontology into a `Schema` (class hierarchy, with
   `owl:equivalentClass` treated as a link in both directions, and property equivalences).
2. Finds, for every source, the properties to read: every property equivalent to
   `name_property`, `latitude_property` and `longitude_property` of `unification.yaml`
   (`Schema.property_family`). This is how the KGE `etype:has_name` is read as a name without
   any KGE-specific code.
3. Reads the entities of every source KG (`read_entities`).
4. Checks that every entity type is an e-type of the teleology and warns otherwise.
5. For every pair under `links` (`link_sources`):
   - **type filter, always on**: two entities are compared only if one type is equal to or a
     subclass of the other (`Schema.compatible`);
   - **primitives**, each switchable in `unification.yaml`, all enabled ones must hold:
     - `coordinates`: haversine distance <= `max_distance_m`;
     - `name`: normalization (`normalize`), then Levenshtein similarity >= `min_similarity`,
       either on the whole names (`levenshtein`) or between the shorter name and the most
       similar part of the longer one (`partial`, see `partial_similarity`);
   - candidates are sorted by score (name similarity + closeness); with `one_to_one` each entity
     keeps only its best candidate;
   - collisions (entities with more than one candidate) are printed.
6. Writes the entity graph (all source KGs unchanged + `owl:sameAs` links) and the report CSV.

`rapidfuzz` is optional: without it a pure Python Levenshtein is used, with the same results.
An alternative configuration can be passed for experiments:
`python src/4_unification.py my_unification.yaml`.

## 5. Where to change what

| I want to... | Change | Code change? |
|---|---|---|
| download a different area | `CENTER`, `RADIUS_M` in `src/1_extraction_osm.py`, then run without `--skip-download` | constants only |
| download more kinds of OSM places | add a rule to `mappings` in `config/osm_source.yaml` **and** add its class to the `osm` `etypes` in `config/pipeline.yaml` | no |
| keep more OSM tags as properties | `tags_as_properties` in `config/osm_source.yaml` | no |
| change how the KGE CSV files are read | `config/kge_source.yaml` (`places`, `place_columns`, `near`, `people`) | no |
| change which e-types the application uses | `etypes` of the source in `config/pipeline.yaml` | no |
| change how KGE is aligned to the unifying ontology | `config/alignment_kge.yaml` | no |
| allow a third alignment operation | `AXIOMS` in `src/2_mapping.py` (and explain it in the alignment files) | yes, small |
| tune the unification (thresholds, normalization, which primitives) | `config/unification.yaml` | no |
| link another pair of sources | `links` in `config/unification.yaml` | no |
| add a new unification primitive (e.g. phone number) | `Settings`, `Entity`, `read_entities` and `link_sources` in `src/4_unification.py`, plus its settings in `config/unification.yaml` | yes |
| change the unifying ontology | `unified_ontology` in `config/pipeline.yaml`; then check every alignment file and `name_property` / `latitude_property` / `longitude_property` in `config/unification.yaml` | no |
| change where the outputs go | `outputs` in `config/pipeline.yaml` (the extraction scripts have their output path as a constant at the top) | constants only |

### Adding a source

1. **Get its KG in RDF.** If it already exists, put the file in the repository. Otherwise
   write a new extraction script `src/1_extraction_<name>.py` (use the two existing ones as
   templates: a configuration file in `config/`, constants at the top, output in `output/`)
   and list it under `extraction` in `config/pipeline.yaml`.
2. **Put its ontology in `ontologies/`.**
3. **If it is not described with the unifying ontology, write `config/alignment_<name>.yaml`**,
   copying the structure of `alignment_kge.yaml`. At least the name and coordinate properties
   must be declared `equivalent` to `osm_ont:name`, `geo:lat` and `geo:long`, otherwise the
   unification cannot read them.
4. **Add it under `sources` in `config/pipeline.yaml`** with `kg`, `ontology`, `etypes` and
   `alignment`.
5. **Add the pair to link under `links` in `config/unification.yaml`.**
6. Run the pipeline and check the output of stage 4: all entity types must be e-types of the
   teleology, and the number of links and collisions must be reasonable.

No change to stages 2-4 is needed.

## 6. Running

```bash
python run_pipeline.py                  # everything, downloading OSM again
python run_pipeline.py --skip-download  # everything, reusing cache/raw_osm_data.pkl

# single stages (from the repository root)
python src/1_extraction_osm.py --skip-download
python src/1_extraction_kge.py
python src/2_mapping.py
python src/3_teleology.py
python src/4_unification.py [other_unification.yaml]
```

Each stage reads the outputs of the previous ones, so after changing a configuration file
rerun from the first stage that reads it.

## 7. Checking the results

- **Console output**: every stage prints what it produced. Stage 2 prints warnings for terms
  missing from an ontology; stage 4 prints entity types missing from the teleology, the
  number of candidate pairs, collisions and links.
- **`output/unification_report.csv`**: one line per link, with name similarity and distance.
  Sort it by similarity or distance to inspect the borderline cases.
- **GraphDB** (ruleset "RDFS-Plus (Optimized)", uncheck "Disable owl:sameAs"):
  - application view: `output/teleology.ttl` + `output/entity_graph.ttl`;
  - schema view: `ontologies/OSM-GTFS-zzz.owl` + `output/teleontology.ttl`.

  With `owl:sameAs` reasoning on, two linked entities share their properties and types as
  inferred statements; in the stored data each keeps its own type. Disable "Include inferred"
  in the Visual graph to see only the stored data.

## 8. Known limitations

- `equivalent` and `child_of` cannot express a union: `etype:bar_pub` (bar or pub) is placed
  under the broader `osm_ont:catering`, so KGE bars and pubs are not compared with OSM bars,
  pubs or cafes.
- OSM coordinates are centroids: for large buildings the distance to the other source can be
  large (MUSE: 78 m). The 100 m threshold accounts for this.
- KGE types with no alignment rule (e.g. `etype:taxi_company`, `etype:tourist`) are never
  unified.
- The downloaded OSM area covers only part of the KGE catalogue (325 of 5556 entities fall
  inside the 2 km radius).
- Links have been checked by sample, not against a reference set.
- Tourists are linked to places by exact name; names that differ slightly are not resolved.
