# Trentino KG Ingestion (iTelos Approach)

This repository contains the scripts for the automated extraction, mapping, and semantic alignment of a Knowledge Graph (KG) starting from OpenStreetMap (OSM) data. The system strictly applies the **iTelos methodology**, utilizing a modular pipeline to formally separate data extraction, Source KG generation (Reference Context), and Entity Unification (Personal Context).

## Repository Contents

### Core Pipeline (iTelos Layers)
To maximize data reusability, ensure URI provenance, and avoid redundant API calls, the execution is split into three independent stages:

* `1_extraction.py`: Handles the extraction of raw geographic data via OSMnx based on predefined filters. Saves the output locally as a serialized dataframe (`raw_osm_data.pkl`).
* `2_mapping.py`: Generates the **Source Knowledge Graph** (`source_kg.nt`). It reads the raw data and maps it strictly using the source OSM ontology, preserving the original OSM URIs (e.g., `http://osm.kg/...`) to guarantee data provenance.
* `3_unification.py`: Performs **Ontology Alignment and Entity Unification**. It links the Source KG physical nodes to the Unified Ontology (e.g., `etype:Restaurant`) and introduces the Personal Context (e.g., `app:Tourist`), generating teleological relationships (e.g., `app:eatsAt`). Outputs the final `final_unified_kg.nt`.
* `run_pipeline.py`: Master script for automated, sequential execution of the entire pipeline with built-in error handling.

### Ontologies & Configuration
To support the inference and the alignment process, the following ontology files are utilized:
* `OSM-GTFS-zzz.owl` (or equivalent source ontology): Defines the base classes (e.g., `openstreetmap_place`) and data properties (e.g., `osm_id`, `name`) for the Source KG.
* `teleontology.ttl`: The Unified Ontology. Defines the generalized class hierarchy (Reference Context) and anchors it to the OSM root class to maintain the inference tree.
* `teleology.ttl`: Defines the Personal Context (`app:Tourist`), along with the strictly typed domains and ranges for teleological Object Properties (`app:isAt`, `app:eatsAt`, etc.).

### Generated Artifacts (Local Only)
Data files are excluded from version control via `.gitignore` due to size and generation frequency:
* `raw_osm_data.pkl`: Intermediate binary file containing raw OSM nodes.
* `source_kg.nt`: N-Triples file containing the pure OSM Source Graph.
* `final_unified_kg.nt`: Final N-Triples file containing the aligned and unified Knowledge Graph.

## Execution Flow

To successfully build and query the Knowledge Graph, follow these steps:

1. **GraphDB Setup:** Before running the pipeline, manually import the source `.owl` ontology, `teleontology.ttl`, and `teleology.ttl` into your GraphDB repository. This builds the foundational schema for inference.
2. **Automated Pipeline Execution:** 
   Run the master script to perform extraction, source KG generation, and unification sequentially:
   ```bash
   python run_pipeline.py