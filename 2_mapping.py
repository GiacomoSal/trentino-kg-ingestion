import pickle
import re
from rdflib import Graph, URIRef, Literal, Namespace
from rdflib.namespace import RDF, XSD

# namespace dell'ontologia base (non toccare quello lungo che sennò salta tutto in GraphDB)
OSM_ONT = Namespace("http://www.semanticweb.org/lixiaoyue/ontologies/2023/2/untitled-ontology-26#")
# base URI per i nodi OSM fisici (serve per la provenance come mi ha detto Davide)
OSM_KG = Namespace("http://osm.kg/") 

def main():
    print("Carico i dati raw dalla cache...")
    try:
        with open("raw_osm_data.pkl", "rb") as f:
            raw_data = pickle.load(f)
    except FileNotFoundError:
        print("Errore: file raw_osm_data.pkl non trovato. Fai girare prima 1_extraction.py")
        return

    kg = Graph()
    kg.bind("osm_ont", OSM_ONT)
    kg.bind("osm", OSM_KG)

    print("Genero il SOG-OSM...")

    # check brutto per capire se raw_data è un df o un dizionario normale
    if hasattr(raw_data, "iterrows"):
        element_list = [row.to_dict() | {"_index_id": index} for index, row in raw_data.iterrows()]
    elif isinstance(raw_data, dict):
        element_list = list(raw_data.values())
    else:
        element_list = raw_data

    for element in element_list:
        # fix per gli id sporchi di OSMnx tipo ('node', 867377379)
        raw_id_str = str(element.get("osmid", element.get("id", element.get("_index_id", ""))))
        
        # piglio solo i numeri con una regex
        numeri = re.findall(r'\d+', raw_id_str)
        if not numeri:
            continue
        clean_osm_id = numeri[-1] 

        # estraggo i tags dall'elemento
        tags = element.get("tags", element)

        # uso l'id vero di osm per non perdere la provenienza del dato
        node_uri = URIRef(f"http://osm.kg/{clean_osm_id}")
        
        # metto la root class sennò in graphdb l'albero si sminchia
        kg.add((node_uri, RDF.type, OSM_ONT.openstreetmap_place))
        
        amenity = tags.get("amenity")
        
        # FIXME: da automatizzare leggendo dal yaml, per ora lascio gli if a mano per testare
        if amenity == "restaurant":
            kg.add((node_uri, RDF.type, OSM_ONT.point_restaurant))
        elif amenity == "cafe":
            kg.add((node_uri, RDF.type, OSM_ONT.point_cafe))
        elif amenity == "pub":
            kg.add((node_uri, RDF.type, OSM_ONT.point_pub))
        
        # cast espliciti dei tipi per evitare rogne in fase di inferenza
        kg.add((node_uri, OSM_ONT.osm_id, Literal(clean_osm_id, datatype=XSD.integer)))
        
        # controllo che il name sia stringa per colpa di pandas che a volte ci sbatte dentro i NaN
        if "name" in tags and isinstance(tags["name"], str):
            kg.add((node_uri, OSM_ONT.name, Literal(tags["name"], datatype=XSD.string)))

    out_file = "source_kg.nt"
    kg.serialize(destination=out_file, format="nt", encoding="utf-8")
    print(f"Finito. Salvato in {out_file}")

if __name__ == "__main__":
    main()