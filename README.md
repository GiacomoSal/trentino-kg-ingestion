# Trentino KG Ingestion (iTelos Approach)

This repository contains the scripts for the automated extraction, mapping, and semantic alignment of a Knowledge Graph (KG) starting from OpenStreetMap (OSM) data. The system strictly applies the **iTelos methodology**, utilizing a modular pipeline to formally separate data extraction, Source KG generation (Reference Context), and Entity Unification (Personal Context)[cite: 2].

## Repository Contents

### Core Pipeline (iTelos Layers)
To maximize data reusability, ensure URI provenance, and avoid redundant API calls, the execution is split into three independent stages[cite: 2]:

* `1_extraction.py`: Handles the extraction of raw geographic data via OSMnx based on predefined filters. Saves the output locally as a serialized dataframe (`raw_osm_data.pkl`)[cite: 2].
* `2_mapping.py`: Generates the **Source Knowledge Graph** (`source_kg.nt`). It reads the raw data and maps it strictly using the source OSM ontology, preserving the original OSM URIs (e.g., `http://osm.kg/...`) to guarantee data provenance[cite: 2].
* `3_unification.py`: Performs **Ontology Alignment and Entity Unification**. It links the Source KG physical nodes to the Unified Ontology (e.g., `etype:Restaurant`) and merges it with the Personal Context loaded from `tourist_profile.ttl`, generating teleological relationships (e.g., `app:eatsAt`)[cite: 2]. Outputs the final `final_unified_kg.nt`[cite: 2].
* `run_pipeline.py`: Master script for automated, sequential execution of the entire pipeline with built-in error handling[cite: 2].

### Ontologies & Configuration
To support the inference and the alignment process, the following ontology files are utilized[cite: 2]:
* `OSM-GTFS-zzz.owl` (or equivalent source ontology): Defines the base classes (e.g., `openstreetmap_place`) and data properties (e.g., `osm_id`, `name`) for the Source KG[cite: 2].
* `teleontology.ttl`: The Unified Ontology. Defines the generalized class hierarchy (Reference Context) and anchors it to the OSM root class to maintain the inference tree[cite: 2].
* `teleology.ttl`: Defines the Personal Context (`app:Tourist`), along with the strictly typed domains and ranges for teleological Object Properties (`app:isAt`, `app:eatsAt`, etc.)[cite: 2].
* `tourist_profile.ttl`: Contains the actual physical data of the user (`app:Tourist`). It is loaded externally by the unification script to completely avoid hardcoding user data into the source code.

### Generated Artifacts (Local Only)
Data files are excluded from version control via `.gitignore` due to size and generation frequency[cite: 2]:
* `raw_osm_data.pkl`: Intermediate binary file containing raw OSM nodes[cite: 2].
* `source_kg.nt`: N-Triples file containing the pure OSM Source Graph[cite: 2].
* `final_unified_kg.nt`: Final N-Triples file containing the aligned and unified Knowledge Graph[cite: 2].

## Execution Flow

To successfully build and query the Knowledge Graph, follow these steps[cite: 2]:

1. **GraphDB Setup:** Before running the pipeline, manually import the source `.owl` ontology, `teleontology.ttl`, and `teleology.ttl` into your GraphDB repository. This builds the foundational schema for inference[cite: 2].
2. **Automated Pipeline Execution:** 
   Run the master script to perform extraction, source KG generation, and unification sequentially[cite: 2]:
   ```bash
   python run_pipeline.py