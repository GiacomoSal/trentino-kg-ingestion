import subprocess
import sys

def run_script(script_name):
    print(f"--- Esecuzione: {script_name} ---")
    try:
        # Uso sys.executable per assicurarmi che giri nel venv corretto
        # e non chiami il python di sistema del Mac per sbaglio
        subprocess.run([sys.executable, script_name], check=True)
    except subprocess.CalledProcessError:
        print(f"[ERRORE] Lo script {script_name} ha restituito un errore.")
        print("Interrompo la pipeline per non generare grafi parziali o corrotti.")
        sys.exit(1)
    except FileNotFoundError:
        print(f"[ERRORE] File {script_name} non trovato. Controllare i path.")
        sys.exit(1)

def main():
    print("Avvio pipeline iTelos...")
    
    # Array con l'ordine di esecuzione forzato
    # NB: se in futuro aggiungiamo step intermedi, vanno inseriti qui
    scripts = [
        "1_extraction.py",
        "2_mapping.py",
        "3_unification.py"
    ]
    
    for script in scripts:
        run_script(script)
        
    print("\n--- Pipeline completata ---")
    print("Output attesi: raw_osm_data.pkl, source_kg.nt, final_unified_kg.nt")
    print("Ricordarsi di fare l'upload di final_unified_kg.nt su GraphDB.")

if __name__ == "__main__":
    main()