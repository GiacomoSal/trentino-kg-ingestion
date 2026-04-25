from rdflib import Graph, URIRef, Namespace
from rdflib.namespace import RDF, OWL

def main():
    print("Caricamento del Source KG (puro OSM)...")
    kg = Graph()
    
    try:
        # Carichiamo il file generato dallo Script 2
        kg.parse("source_kg.nt", format="nt")
    except FileNotFoundError:
        print("Errore: source_kg.nt non trovato. Lancia prima lo script 2!")
        return

    # --- NAMESPACE ---
    OSM_ONT = Namespace("http://www.semanticweb.org/lixiaoyue/ontologies/2023/2/untitled-ontology-26#")
    OSM_KG = Namespace("http://osm.kg/")
    # TODO: Verifica che questi URI corrispondano esattamente a quelli nei tuoi file .ttl!
    ETYPE = Namespace("http://teleology.kg/etype#") 
    APP = Namespace("http://teleology.kg/app#")

    print("Inizio fase di Allineamento (Teleontologia) e Unificazione (Teleologia)...")

    # 1. Creiamo il Personal Context (Il Turista)
    tourist_uri = URIRef("http://teleology.kg/tourist/Giacomo")
    kg.add((tourist_uri, RDF.type, APP.Tourist))

    # 2. Cicliamo sui nodi per fare il Mapping e l'Unificazione
    # Usiamo una lista per non modificare il grafo mentre lo cicliamo
    for s, p, o in list(kg):
        
        # Se il nodo è un ristorante in OSM...
        if p == RDF.type and o == OSM_ONT.point_restaurant:
            # ALLINEAMENTO: Diciamo che questo nodo fisico è ANCHE un etype:Restaurant
            kg.add((s, RDF.type, ETYPE.Restaurant))
            # TELEOLOGIA: Il turista interagisce con questo nodo
            kg.add((tourist_uri, APP.eatsAt, s))

        # Se il nodo è un bar in OSM...
        elif p == RDF.type and o == OSM_ONT.point_bar:
            kg.add((s, RDF.type, ETYPE.Bar))
            kg.add((tourist_uri, APP.drinksAt, s))

        # Se il nodo è un cafe in OSM...
        elif p == RDF.type and o == OSM_ONT.point_cafe:
            kg.add((s, RDF.type, ETYPE.Cafe))
            kg.add((tourist_uri, APP.drinksAt, s))
            
        # Se il nodo è un pub in OSM...
        elif p == RDF.type and o == OSM_ONT.point_pub:
            kg.add((s, RDF.type, ETYPE.Pub))
            kg.add((tourist_uri, APP.drinksAt, s))

    # Salvo il Knowledge Graph Finale Unificato
    out_file = "final_unified_kg.nt"
    kg.serialize(destination=out_file, format="nt", encoding="utf-8")
    print(f"Finito! Grafo unificato salvato con successo in {out_file}")
    print("Ora puoi caricare final_unified_kg.nt in GraphDB per le tue query SPARQL!")

if __name__ == "__main__":
    main()