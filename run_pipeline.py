import subprocess
import sys

def run_script(script_name):
    """Executes a python script sequentially and halts on error."""
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
    # iTelos pipeline sequential execution: Extraction -> Source Mapping -> Unification
    scripts = [
        "1_extraction.py",
        "2_mapping.py",
        "3_unification.py"
    ]
    
    for script in scripts:
        run_script(script)
        
    print("\n[+] Pipeline execution completed successfully.")
    print("[!] Action required: Load 'source_kg.nt', 'tourist_profile.ttl', and 'unified_kg.ttl' into GraphDB.")

if __name__ == "__main__":
    main()