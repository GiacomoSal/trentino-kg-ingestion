import pickle
import re
from rdflib import Graph, URIRef, Literal, Namespace
from rdflib.namespace import RDF, XSD

# Setto i namespace. 
# Questo è quello lunghissimo del file .owl sorgente (guai a toccarlo)
OSM_ONT = Namespace("http://www.semanticweb.org/lixiaoyue/ontologies/2023/2/untitled-ontology-26#")
# Base URI per i nodi fisici di OSM (richiesto da Davide per mantenere la provenance)
OSM_KG = Namespace("http://osm.kg/") 

def main():
    print("Carico i dati raw dalla cache...")
    try:
        with open("raw_osm_data.pkl", "rb") as f:
            raw_data = pickle.load(f)
    except FileNotFoundError:
        print("Errore: file raw_osm_data.pkl non trovato. Esegui prima 1_extraction.py!")
        return

    # Inizializzo il grafo per il Source KG
    kg = Graph()
    kg.bind("osm_ont", OSM_ONT)
    kg.bind("osm", OSM_KG)

    print("Genero il Source KG puro (mappo solo la roba di OSM)...")

    # Normalizziamo raw_data per assicurarci di ciclare sulle righe e non sui nomi delle colonne
    if hasattr(raw_data, "iterrows"):
        # Se è un DataFrame Pandas, estraiamo le righe come dizionari
        element_list = [row.to_dict() | {"_index_id": index} for index, row in raw_data.iterrows()]
    elif isinstance(raw_data, dict):
        # Se è un dizionario
        element_list = list(raw_data.values())
    else:
        # Se è già una lista
        element_list = raw_data

    for element in element_list:
        # Prendo l'ID grezzo che OSMnx/Pandas restituisce sporco, es: "('node', 867377379)"
        raw_id_str = str(element.get("osmid", element.get("id", element.get("_index_id", ""))))
        
        # Estraggo chirurgicamente solo la parte numerica tramite regex
        numeri = re.findall(r'\d+', raw_id_str)
        if not numeri:
            continue
        clean_osm_id = numeri[-1] # Prende l'ultimo blocco di numeri (l'ID vero e proprio)

        # Se non c'è una chiave 'tags' specifica, l'intera riga funge da dizionario dei tag
        tags = element.get("tags", element)

        # 1. URI DEL NODO: uso l'OSM ID direttamente come da specifiche (es: http://osm.kg/867377379)
        # Evito di creare URI inventati, così non perdo la provenienza del dato
        node_uri = URIRef(f"http://osm.kg/{clean_osm_id}")
        
        # 2. RADICE: imposto sempre la classe root per non spaccare l'albero di inferenza in GraphDB
        kg.add((node_uri, RDF.type, OSM_ONT.openstreetmap_place))
        
        # 3. CLASSI SPECIFICHE (Type)
        # Becco i tag e assegno la classe specifica del .owl
        amenity = tags.get("amenity")
        
        # TODO: andrà automatizzato leggendo dal file yaml, 
        # per ora hardcodo alcuni tipi per verificare che GraphDB prenda i dati
        if amenity == "restaurant":
            kg.add((node_uri, RDF.type, OSM_ONT.point_restaurant))
        elif amenity == "cafe":
            kg.add((node_uri, RDF.type, OSM_ONT.point_cafe))
        elif amenity == "pub":
            kg.add((node_uri, RDF.type, OSM_ONT.point_pub))
        
        # 4. DATA PROPERTIES SORGENTI
        # Aggiungo l'id come integer e il name testuale
        kg.add((node_uri, OSM_ONT.osm_id, Literal(clean_osm_id, datatype=XSD.integer)))
        
        # isinstance(..., str) evita che Pandas converta nomi vuoti in float (NaN) facendoci crashare
        if "name" in tags and isinstance(tags["name"], str):
            kg.add((node_uri, OSM_ONT.name, Literal(tags["name"], datatype=XSD.string)))

    # Salvo il dump
    out_file = "source_kg.nt"
    kg.serialize(destination=out_file, format="nt", encoding="utf-8")
    print(f"Finito. Grafo salvato con successo in {out_file}")

if __name__ == "__main__":
    main()