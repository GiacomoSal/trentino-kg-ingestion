import os
import subprocess
import sys

STAGES = [
    "1_extraction.py",       # OSM data via osm2kg -> raw_osm_data.pkl
    "1_extraction_kge.py",   # KGE CSV files -> kge_kg.ttl
    "2_mapping.py",          # osm_kg.ttl + teleontology.ttl
    "3_unification.py",      # unified_kg.ttl + unification_report.csv
]


def run_script(script_name):
    """Executes a python script and halts the pipeline on error."""
    print(f"\n--- Starting {script_name} ---")
    try:
        # sys.executable ensures the script runs in the current virtual environment
        subprocess.run([sys.executable, script_name], check=True)
    except subprocess.CalledProcessError:
        print(f"[ERROR] {script_name} failed. Halting pipeline to prevent partial graph generation.")
        sys.exit(1)
    except FileNotFoundError:
        print(f"[ERROR] Script {script_name} not found.")
        sys.exit(1)


def main():
    # --skip-download reuses raw_osm_data.pkl instead of querying OSM again
    skip_download = "--skip-download" in sys.argv
    for script in STAGES:
        if script == "1_extraction.py" and skip_download:
            if not os.path.exists("raw_osm_data.pkl"):
                print("[ERROR] --skip-download needs raw_osm_data.pkl")
                sys.exit(1)
            print("\n--- Skipping 1_extraction.py (using raw_osm_data.pkl) ---")
            continue
        run_script(script)

    print("\n[+] Pipeline execution completed successfully.")
    print("[!] Load into GraphDB: OSM-GTFS-zzz.owl, kge_ontology.owl, teleontology.ttl, unified_kg.ttl")


if __name__ == "__main__":
    main()
