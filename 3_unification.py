import rdflib
from rdflib import Graph, Namespace
from rdflib.namespace import RDF

def main():
    print("Avvio fase di Entity Unification e Teleologia...")

    # Inizializzazione grafo
    g = Graph()
    
    # 1. Definisco i Namespace ESATTI usati nei vari file
    APP = Namespace("http://knowdive.disi.unitn.it/trentino-app#")
    # Questo è il namespace che usi in 2_mapping.py per le classi fisiche
    OSM_ONT = Namespace("http://www.semanticweb.org/lixiaoyue/ontologies/2023/2/untitled-ontology-26#")
    
    g.bind("app", APP)
    g.bind("osm_ont", OSM_ONT)

    # 2. Caricamento dei contesti
    try:
        g.parse("source_kg.nt", format="nt")
        print("Reference Context (source_kg.nt) caricato con successo.")
    except FileNotFoundError:
        print("Errore: file source_kg.nt non trovato. Esegui prima lo script 2.")
        return

    try:
        g.parse("tourist_profile.ttl", format="turtle")
        print("Personal Context (tourist_profile.ttl) caricato con successo.")
    except FileNotFoundError:
        print("Errore: file tourist_profile.ttl non trovato.")
        return

    # 3. Identificazione semantica del Turista
    tourist_uri = None
    for s in g.subjects(RDF.type, APP.Tourist):
        tourist_uri = s
        break 

    if not tourist_uri:
        print("Attenzione: Nessuna istanza di app:Tourist trovata nel grafo.")
        return

    # 4. Creazione delle relazioni teleologiche (Formali)
    ristoranti_trovati = 0
    bar_trovati = 0

    # Definisco le classi target basandomi ESATTAMENTE su come le salva lo script 2_mapping.py
    target_restaurants = [OSM_ONT.point_restaurant]
    target_bars = [OSM_ONT.point_cafe, OSM_ONT.point_pub]

    # Cerco i ristoranti e creo l'arco eatsAt
    for target_class in target_restaurants:
        for entity_uri in g.subjects(RDF.type, target_class):
            g.add((tourist_uri, APP.eatsAt, entity_uri))
            ristoranti_trovati += 1

    # Cerco i bar/pub/cafe e creo l'arco drinksAt
    for target_class in target_bars:
        for entity_uri in g.subjects(RDF.type, target_class):
            g.add((tourist_uri, APP.drinksAt, entity_uri))
            bar_trovati += 1

    # 5. Salvataggio del Knowledge Graph unificato finale
    output_file = "final_unified_kg.nt"
    g.serialize(destination=output_file, format="nt")
    
    print("\n--- Risultati Unificazione ---")
    print(f"Turista allineato: {tourist_uri}")
    print(f"Archi 'eatsAt' generati: {ristoranti_trovati}")
    print(f"Archi 'drinksAt' generati: {bar_trovati}")
    print(f"Grafo finale salvato in: {output_file}")

if __name__ == "__main__":
    main()