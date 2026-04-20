from rdflib import Graph, Namespace
from rdflib.namespace import RDF
import requests

GRAPHDB_URL = "http://192.168.178.33:7200/repositories/Mio_Reference_KG/statements"

print("Caricamento del grafo mappato...")
kg = Graph()
kg.parse("mapped_kg.nt", format="nt")

# Definisci i namespace necessari
APP = Namespace("http://knowdive.disi.unitn.it/trentino-app#")
ETYPE = Namespace("http://knowdive.disi.unitn.it/etype#")

# Inserimento Personal Context ed Entity Unification (usando per ora un match statico/coordinate)
turista_uri = APP["Tourist/Giacomo"]
kg.add((turista_uri, RDF.type, APP.Tourist))

# Trova un nodo ristorante (puoi rendere questa logica più furba usando le coordinate come diceva Davide)
primo_food = None
for s, p, o in kg.triples((None, RDF.type, ETYPE.restaurant)):
    primo_food = s
    break

if primo_food:
    kg.add((turista_uri, APP.eatsAt, primo_food))
    print(f"Unificazione completata: Turista collegato a {primo_food}")

print("Invio KG unificato a GraphDB...")
response = requests.post(GRAPHDB_URL, data=kg.serialize(format="nt"), headers={'Content-Type': 'application/n-triples'})

if response.status_code == 204:
    print("Successo: Dati caricati correttamente su GraphDB.")
else:
    print(f"Errore HTTP {response.status_code}.")