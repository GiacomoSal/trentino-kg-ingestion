# Trentino KG Ingestion

This repository contains the scripts for the ingestion and automated generation of the Entity Graph (EG) starting from OpenStreetMap data, applying the iTelos approach to separate the ontological levels.

## Repository Contents

* `generate_clean_kg.py`: Main script. It performs data extraction via OSMnx (using the core functions of osm2kg) and generates the Entity Graph by dynamically applying the mapping rules.
* `mapping.yaml`: Declarative configuration file that maps OSM tags to the ETG classes (e.g., `etype:restaurant`).
* `teleontology.ttl`: Static file containing the class hierarchies (Reference and Personal Context).
* `teleology.ttl`: Static file containing the properties (e.g., `app:eatsAt`).

## Execution Notes
The `generate_clean_kg.py` script bypasses the previous RDF generator to avoid embedding class hierarchies directly into the data (thus formally separating the EG from the Teleontology/Teleology). The output data is sent directly to a local GraphDB instance.