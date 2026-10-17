"""Runs the whole pipeline in order and stops at the first error.

The extraction scripts are source-specific and listed in pipeline.yaml; the other
stages are generic. Run it from the repository root. --skip-download is passed to the extraction scripts so that
OSM data is read from the local cache instead of being downloaded again.
"""
import subprocess
import sys

import yaml

GENERIC_STAGES = [
    "src/2_mapping.py",      # teleontology
    "src/3_teleology.py",    # teleology, from the teleontology
    "src/4_unification.py",  # entity graph (built on the teleology) + report
]


def run_script(script_name, args=()):
    """Executes a python script and halts the pipeline on error."""
    print(f"\n--- Starting {script_name} ---")
    try:
        # sys.executable ensures the script runs in the current virtual environment
        subprocess.run([sys.executable, script_name, *args], check=True)
    except subprocess.CalledProcessError:
        print(f"[ERROR] {script_name} failed. Halting pipeline to prevent partial graph generation.")
        sys.exit(1)
    except FileNotFoundError:
        print(f"[ERROR] Script {script_name} not found.")
        sys.exit(1)


def main():
    with open("config/pipeline.yaml", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    extraction_args = ["--skip-download"] if "--skip-download" in sys.argv else []
    for script in cfg.get("extraction", []):
        run_script(script, extraction_args)
    for script in GENERIC_STAGES:
        run_script(script)

    out = cfg["outputs"]
    print("\n[+] Pipeline execution completed successfully.")
    print(f"[!] GraphDB, application view: {out['teleology']} + {out['entity_graph']}")
    print(f"[!] GraphDB, schema view: {cfg['unified_ontology']} + {out['teleontology']}")


if __name__ == "__main__":
    main()
