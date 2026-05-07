import subprocess
import sys

def run_script(script_name):
    print(f"--- Faccio partire {script_name} ---")
    try:
        # sys.executable serve per usare il python del venv
        # altrimenti su mac usa quello di sistema e non trova mezza libreria
        subprocess.run([sys.executable, script_name], check=True)
    except subprocess.CalledProcessError:
        print(f"[ERR] {script_name} è andato in errore.")
        print("Stoppo tutto, sennò faccio casini col grafo parziale.")
        sys.exit(1)
    except FileNotFoundError:
        print(f"[ERR] Non trovo {script_name}.")
        sys.exit(1)

def main():
    # Ordine degli step come da paper iTelos: estrazione -> sorgente -> unificazione
    scripts = [
        "1_extraction.py",
        "2_mapping.py",
        "3_unification.py" # questo adesso usa i doppi nodi e il sameAs come chiesto da Davide
    ]
    
    for script in scripts:
        run_script(script)
        
    print("Tutto fatto. Ricordarsi di caricare final_unified_kg.nt su GraphDB.")

if __name__ == "__main__":
    main()