import rdflib
from rdflib import Graph, Namespace, URIRef
from rdflib.namespace import RDF, OWL

def main():
    print("Avvio Entity Unification (doppi nodi e sameAs)...")

    g_osm = Graph()
    g_tourist = Graph()
    g_final = Graph()
    
    APP = Namespace("http://knowdive.disi.unitn.it/trentino-app#")
    OSM_ONT = Namespace("http://www.semanticweb.org/lixiaoyue/ontologies/2023/2/untitled-ontology-26#")
    SCHEMA = Namespace("http://schema.org/")
    ETYPE = Namespace("http://teleology.kg/etype#")

    # bind dei prefissi così il file finale si legge bene e non ha URI chilometrici
    g_final.bind("app", APP)
    g_final.bind("osm_ont", OSM_ONT)
    g_final.bind("schema", SCHEMA)
    g_final.bind("etype", ETYPE)
    g_final.bind("owl", OWL)

    try:
        g_osm.parse("source_kg.nt", format="nt")
    except FileNotFoundError:
        print("Manca source_kg.nt")
        return

    try:
        g_tourist.parse("tourist_profile.ttl", format="turtle")
    except FileNotFoundError:
        print("Manca tourist_profile.ttl")
        return

    # unisco fisicamente i grafi (ma non ci sono ancora le relazioni logiche in mezzo)
    g_final += g_osm
    g_final += g_tourist

    match_trovati = 0
    print("Cerco corrispondenze nome-nome tra Turista e OSM...")

    # mi ciclo tutti i posti salvati nel profilo utente (personal context)
    for personal_node, _, place_name in g_tourist.triples((None, SCHEMA.name, None)):
        
        # metto tutto lower e strip per evitare che un cazzo di spazio mi faccia saltare il match
        name_str = str(place_name).lower().strip()

        # cerco nel grafo di osm se c'è un posto fisico che si chiama uguale
        for osm_node, _, osm_name in g_osm.triples((None, OSM_ONT.name, None)):
            
            if str(osm_name).lower().strip() == name_str:
                # trovato! piazzo il sameAs per unire l'entità utente a quella fisica. Provenance salva.
                g_final.add((personal_node, OWL.sameAs, osm_node))
                match_trovati += 1
                print(f"[ MATCH ] {name_str} -> {osm_node}")

    output_file = "final_unified_kg.nt"
    g_final.serialize(destination=output_file, format="nt")
    
    print(f"Finito. Trovati {match_trovati} match (owl:sameAs).")

if __name__ == "__main__":
    main()