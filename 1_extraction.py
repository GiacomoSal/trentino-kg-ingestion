import os
import yaml
import pandas as pd
import osm2kg as og

def load_tags_from_yaml(filepath="mapping.yaml"):
    """Reads mapping.yaml and dynamically builds the tags dictionary for OSM extraction."""
    if not os.path.exists(filepath):
        print(f"[WARNING] {filepath} not found. Using default restaurant tags.")
        return {'amenity': ['restaurant']}
        
    with open(filepath, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
        
    tags_to_download = {}
    mappings = config.get("mappings", [])
    
    # Iterate through YAML rules and aggregate OSM tags
    for rule in mappings:
        filters = rule.get("osm_filter", {})
        for osm_key, values in filters.items():
            if osm_key not in tags_to_download:
                tags_to_download[osm_key] = []
            # Extend the list avoiding duplicates
            for v in values:
                if v not in tags_to_download[osm_key]:
                    tags_to_download[osm_key].append(v)
                    
    return tags_to_download

def main():
    # Trento city center coordinates
    centro_trento = (46.0678, 11.1211)
    raggio_metri = 2000

    print("Parsing dynamic tags from mapping.yaml...")
    tags_to_download = load_tags_from_yaml()
    print(f"Tags to extract: {tags_to_download}")

    print("Downloading OSM data via osm2kg...")
    # Extract features matching the dynamically loaded tags
    gdf = og.feature.features_from_point(centro_trento, tags=tags_to_download, dist=raggio_metri)
    
    # Filter to keep only entities with a name
    gdf = og.feature.filter_gdf(gdf, including_filters={"name": True})

    # Save raw data locally to avoid hitting OSM API on every test run
    cache_filename = "raw_osm_data.pkl"
    gdf.to_pickle(cache_filename)
    print(f"Data successfully extracted and cached in {cache_filename}")

if __name__ == "__main__":
    main()