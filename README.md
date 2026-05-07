# Trentino KG Ingestion (iTelos Approach)

This repository contains the scripts for the automated extraction, mapping, and semantic alignment of a Knowledge Graph (KG) starting from OpenStreetMap (OSM) data. The system strictly applies the **iTelos methodology**, utilizing a modular pipeline to formally separate data extraction, Source KG generation (Reference Context), and Entity Unification (Personal Context).

## Repository Contents

### Core Pipeline (iTelos Layers)
To maximize data reusability, ensure URI provenance, and avoid redundant API calls, the execution is split into three independent stages:

* `1_extraction.py`: Handles the extraction of raw geographic data via OSMnx based on predefined filters. Saves the output locally as a serialized dataframe (`raw_osm_data.pkl`).
* `2_mapping.py`: Generates the **Source Knowledge Graph** (`source_kg.nt`). It reads the raw data and maps it strictly using the source OSM ontology, preserving the original OSM URIs (e.g., `http://osm.kg/...`) to guarantee data provenance.
* `3_unification.py`: Performs **Ontology Alignment and Entity Unification**. It implements strict semantic equivalence: rather than directly linking the user to physical nodes, it matches places from the Personal Context (`tourist_profile.ttl`) with the Reference Context (OSM) using the `owl:sameAs` property. Outputs the final `final_unified_kg.nt`.
* `run_pipeline.py`: Master script for automated, sequential execution of the entire pipeline.

### Ontologies & Configuration
To support inference and the alignment process, the following ontology files are utilized:
* `OSM-GTFS-zzz.owl` (or equivalent source ontology): Defines the base classes (e.g., `openstreetmap_place`) and data properties for the Source KG.
* `teleontology.ttl`: The Unified Ontology. Defines the generalized class hierarchy (Reference Context) and anchors it to the OSM classes via `rdfs:subClassOf` to automatically inherit spatial properties.
* `teleology.ttl`: Defines the Personal Context (`app:Tourist`), along with the strictly typed domains and ranges for teleological Object Properties (`app:isAt`, `app:eatsAt`, etc.).
* `tourist_profile.ttl`: Contains the actual physical data of the user (`app:Tourist`) and their personal instances of places. 
* `mapping.yaml`: Declarative configuration file for schema-driven mapping.

### Generated Artifacts (Local Only)
Data files are excluded from version control via `.gitignore` due to size and generation frequency:
* `raw_osm_data.pkl`: Intermediate binary file.
* `source_kg.nt`: Pure OSM Source Graph.
* `final_unified_kg.nt`: Final unified Knowledge Graph.

## Execution Flow

To successfully build and query the Knowledge Graph:

1. **Automated Pipeline Execution:** Run the master script to generate the final N-Triples by executing: python run_pipeline.py

2. **GraphDB Setup:** In GraphDB, go to Import -> RDF and upload the complete suite to enable full inference:
   * `OSM-GTFS-zzz.owl`
   * `teleontology.ttl`
   * `teleology.ttl`
   * `tourist_profile.ttl`
   * `final_unified_kg.nt`