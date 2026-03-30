import geopandas as gpd
import requests
from rdflib.namespace import RDFS, XSD, RDF
from rdflib import Namespace, Graph, URIRef
from rdflib.term import BNode
import shapely
import osm2kg.sparql_queries as sq
import osm2kg.settings as settings


RDF4J_SERVER = settings.RDF4J_SERVER
BIG_ONTO_REPO = settings.OSM_ONTO_REPO
SCHEMA = settings.SCHEMA
INDIVIDUAL = settings.INDIVIDUAL
BASE = settings.BASE
ROUTE = settings.ROUTE

# added_classes = set()
# added_obj_properties = set()
# added_data_properties = set()


# === Import .rdf file in a Repository ===
def import_rdf_file(repo_id:str, rdf_file_path:str):
    """
    Import a rdf file inside a repository in RDF4J server.

    Parameters
    ----------
    repo_id : str
        The ID of the repository where to import the RDF file.
    rdf_file_path : str
        The path to the RDF file to import.
    Returns
    -------
    bool
        True if the import was successful, False otherwise.
    """
    with open(rdf_file_path, "rb") as f:
        data = f.read()
        headers = {"Content-Type": "application/rdf+xml"}
        r = requests.post(repo_id, data=data, headers=headers)
        if r.ok:
            print(f"✅ File owl_path imported in repository {repo_id}")
            return True
        else:
            print(f"❌ Error importing: {r.status_code} – {r.text}")
            return False


# === Create and delete Repository ===
def create_repository(repo_id: str, repo_title: str, persist=True, sync_delay=0):
    """
    Create a new RDF4J repository using a Turtle configuration.
    
    Parameters
    ----------
    repo_id : str
        The ID of the repository to create.
    repo_title : str
        The title of the repository to create.
    persist : bool, optional
        Whether to persist the repository to disk, by default True.
    sync_delay : int, optional
        The sync delay in seconds, by default 0.
    Returns
    -------
    bool
        True if the repository was created successfully, False otherwise.
    """
    turtle_config = f"""
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>.
@prefix rep: <http://www.openrdf.org/config/repository#>.
@prefix sr: <http://www.openrdf.org/config/repository/sail#>.
@prefix sail: <http://www.openrdf.org/config/sail#>.
@prefix ms: <http://www.openrdf.org/config/sail/memory#>.
@prefix config: <http://www.openrdf.org/config/sail/memory#>.

[] a rep:Repository ;
   rep:repositoryID "{repo_id}" ;
   rdfs:label "{repo_title}" ;
   rep:repositoryImpl [
      rep:repositoryType "openrdf:SailRepository" ;
      sr:sailImpl [
         sail:sailType "openrdf:MemoryStore" ;
         ms:persist {str(persist).lower()} ;
         ms:syncDelay {sync_delay} ;
         config:iterationCacheSyncThreshold 10000 ;
         config:defaultQueryEvaluationMode "STRICT"
      ]
   ].
"""
    r = requests.put(
        f"{RDF4J_SERVER}/repositories/{repo_id}",
        data=turtle_config.encode("utf-8"),
        headers={"Content-Type": "text/turtle"}
    )
    if not r.ok:
        print(f"❌ Error creating repository: {r.status_code} - {r.text}")
        return False
    return True


def delete_repository(repo_id):
    """
    Delete a repository by its ID

    Parameters
    ----------

    repo_id : str
        The ID of the repository to delete
    
    Returns
    -------
    bool
        True if the repository was deleted successfully, False otherwise
    """
    r = requests.delete(f"{RDF4J_SERVER}/repositories/{repo_id}")
    if r.status_code == 204:
        return True
    else:
        print(f"❌ Error deleting repository: {r.status_code} - {r.text}")
        return False

    
# === Existance of class and property ===

def class_exists_in_repo(class_uri, repo_id):
    """
    Check if a class exists in a repository
    
    Parameters
    ----------
    class_uri : str
        The URI of the class to check
    repo_id : str
        The ID of the repository to check in

    Returns
    -------
    bool
        True if the class exists in the repository, False otherwise"""
    sparql = sq.get_class_existence_query(class_uri)
    r = requests.post(
        f"{RDF4J_SERVER}/repositories/{repo_id}",
        data={"query": sparql},
        headers={"Accept": "application/sparql-results+json"}
    )
    return r.json()["boolean"]


def property_exists_in_repo(prop_uri, repo_id):
    """
    Check if a property exists in a repository
    
    Parameters
    ----------
    prop_uri : str
        The URI of the property to check
    repo_id : str
        The ID of the repository to check in
    
    Returns
    -------
    bool
        True if the property exists in the repository, False otherwise
    """
    sparql = sq.get_property_existence_query(prop_uri)
    r = requests.post(
        f"{RDF4J_SERVER}/repositories/{repo_id}",
        data={"query": sparql},
        headers={"Accept": "application/sparql-results+json"}
    )
    return r.json()["boolean"]


# === Create class and property ===

def get_class_hierarchy_and_axioms(class_uri):
    """
    Given a class URI, extract the class hierarchy and all axioms about the class
    
    Parameters
    ----------
    class_uri : str
        The URI of the class to extract the hierarchy and axioms for
    
    Returns
    -------
    str
        The Turtle serialization of the class hierarchy and axioms
    """
    sparql = sq.get_class_hierarchy_query(class_uri)
    r = requests.post(
        f"{RDF4J_SERVER}/repositories/{BIG_ONTO_REPO}",
        data={"query": sparql},
        headers={"Accept": "text/turtle"}
    )
    return r.text  # Turtle serialization


def construct_property_domain(prop_uri, big_repo_id, repo_id):
    """
    Given a property URI, extract the domain of the property

    Parameters
    ----------
    prop_uri : str
        The URI of the property to extract the domain for
    big_repo_id : str
        The ID of the repository to extract the domain from
    repo_id : str
        The ID of the repository to insert the domain into
    
    Returns
    -------
    bool
        True if the domain was successfully inserted, False otherwise
    """
    sparql = sq.get_property_domain(prop_uri)
    r = requests.post(
        f"{RDF4J_SERVER}/repositories/{big_repo_id}",
        data={"query": sparql},
        headers={"Accept": "application/sparql-results+json"}
    )
    results = r.json().get("results", {}).get("bindings", [])
    # if results type is uri extract value
    for result in results:
        if result["domain"]["type"] == "uri":
            # print(result)
            domain_uri = result["domain"]["value"]
            sparql_construct = sq.get_class_hierarchy_query(domain_uri)
            r2 = requests.post(
                f"{RDF4J_SERVER}/repositories/{big_repo_id}",
                data={"query": sparql_construct},
                headers={"Accept": "text/turtle"}
            )
            if insert_turtle_in_repo(r2.text, repo_id) != True:
                return False
    return True


def construct_property_range(prop_uri, big_repo_id, repo_id):
    """
    Given a property URI, extract the range of the property
    
    Parameters
    ----------
    prop_uri : str
        The URI of the property to extract the range for
    big_repo_id : str
        The ID of the repository to extract the range from
    repo_id : str
        The ID of the repository to insert the range into

    Returns
    -------
    bool
        True if the range was successfully inserted, False otherwise
    """
    sparql = sq.get_property_range(prop_uri)
    r = requests.post(
        f"{RDF4J_SERVER}/repositories/{big_repo_id}",
        data={"query": sparql},
        headers={"Accept": "application/sparql-results+json"}
    )
    results = r.json().get("results", {}).get("bindings", [])
    # if results type is uri extract value
    for result in results:
        if result["range"]["type"] == "uri":
            range_uri = result["range"]["value"]
            sparql_construct = sq.get_class_hierarchy_query(range_uri)
            r2 = requests.post(
                f"{RDF4J_SERVER}/repositories/{big_repo_id}",
                data={"query": sparql_construct},
                headers={"Accept": "text/turtle"}
            )
            return insert_turtle_in_repo(r2.text, repo_id)


def construct_property(prop_uri, big_repo_id, repo_id):
    """
    Given a property URI, extract the domain, the range, and all triples about the property itself, then insert them into the target repository

    Parameters
    ----------
    prop_uri : str
        The URI of the property to construct
    big_repo_id : str
        The ID of the repository to extract the property from
    repo_id : str
        The ID of the repository to insert the property into
    
    Returns
    -------
    bool
        True if the property was successfully constructed and inserted, False otherwise
    """
    if construct_property_domain(prop_uri, big_repo_id, repo_id) is False:
        return False
    if construct_property_range(prop_uri, big_repo_id, repo_id) is False:
        return False
    sparql = sq.get_base_property(prop_uri)
    r = requests.post(
        f"{RDF4J_SERVER}/repositories/{big_repo_id}",
        data={"query": sparql},
        headers={"Accept": "text/turtle"}
    )
    return insert_turtle_in_repo(r.text, repo_id)
    

# === Insert Turtle in Repository ===
def insert_turtle_in_repo(turtle_str, repo_id):
    """
    Insert a Turtle string into a repository

    Parameters
    ----------
    turtle_str : str
        The Turtle string to insert
    repo_id : str
        The ID of the repository to insert the Turtle string into
    
    Returns
    -------
    bool
        True if the Turtle string was successfully inserted, False otherwise
    """
    r = requests.post(
        f"{RDF4J_SERVER}/repositories/{repo_id}/statements",
        data=turtle_str.encode("utf-8"),
        headers={"Content-Type": "text/turtle"}
            )
    if not r.ok:
        print(f"❌ Error inserting ontology: {r.status_code} - {r.text} - {turtle_str} ")
        return False
    return True


# === Process GDF and Add Entities ===
def add_entity(row:gpd.GeoSeries, class_uri, repo_id, comment=None):
    """
    Add an entity to a repository, given a GeoSeries row and the class URI of the entity, for now
    only the 'name' property and the geometry are added as properties.
    It also let to add a comment to the entity. This is used for the anlysis, when isochrone or k-nearest features are calculated.
    The function can be extended to add more properties if needed. It's important that the index of the GeoSeries is set a multiLevelIndex
    for compatibility with the OSM data structure (element, id).

    Parameters
    ----------
    row : gpd.GeoSeries
        The GeoSeries row containing the entity data
    class_uri : str
        The URI of the class of the entity
    repo_id : str
        The ID of the repository to insert the entity into
    comment : str, optional
        A comment to add to the entity, by default None
    
    Returns
    -------
    bool
        True if the entity was successfully added, False otherwise
    """
    property_map = {
        "name": "name",
    }
    uri = f"{INDIVIDUAL}#{row.name[0]}-{row.name[1]}"
    name = str(row.get("name", "")).replace('"', "'")
    triples = [
        f"<{uri}> a <{class_uri}> ;"
    ]
    for prop in property_map.keys():
        if prop in row and row[prop] is not None:
            prop_uri = f"{BASE}#{property_map[prop]}"
            # if prop_uri not in added_obj_properties:
            if not property_exists_in_repo(prop_uri, repo_id):
                construct_property(prop_uri, BIG_ONTO_REPO, repo_id)
                # added_obj_properties.add(prop_uri)
            if prop == "name":
                triples.append(f'<{prop_uri}> "{name}"^^xsd:string ;')

    # if f"{SCHEMA}geo" not in added_obj_properties:
    if not property_exists_in_repo(f"{SCHEMA}geo", repo_id):
        construct_property(f"{SCHEMA}geo", BIG_ONTO_REPO, repo_id)
        construct_property("http://www.co-ode.org/ontologies/ont.owl#GO_6000006", BIG_ONTO_REPO, repo_id) # this name was given by the ontology provided for this project, it indicastes a shapeCoordinates, used to describe the coordinates of a geo shape
        # added_obj_properties.add(f"{SCHEMA}geo")

    geom = row.geometry
    geoShapeIndividual = f"{BASE}#location-{row.name[0]}-{row.name[1]}"
    if geom.geom_type == "Point":
        coords = shapely.to_geojson(row.geometry)
        insert_turtle_in_repo(f"""<{geoShapeIndividual}> a <{SCHEMA}GeoShape> ; <http://www.co-ode.org/ontologies/ont.owl#GO_6000006> '{coords}'^^xsd:string .""", repo_id)
        triples.append(f'    <{SCHEMA}geo> <{geoShapeIndividual}> ;')
    else:
        coords = shapely.to_geojson(row.geometry)
        insert_turtle_in_repo(f"""<{geoShapeIndividual}> a <{SCHEMA}GeoShape> ; <http://www.co-ode.org/ontologies/ont.owl#GO_6000006> '{coords}'^^xsd:string  .""", repo_id)
        triples.append(f'    <{SCHEMA}geo> <{geoShapeIndividual}> ;')
    if comment:
        triples.append(f'<{RDFS.comment}> "{comment}"^^xsd:string ;')
    # Remove trailing semicolon and add dot
    triples[-1] = triples[-1].rstrip(";") + " ."
    insert_query = sq.get_insert_data_query(str(SCHEMA), str(XSD), triples)
    # print(" ".join(triples))
    r = requests.post(
        f"{RDF4J_SERVER}/repositories/{repo_id}/statements",
        data=insert_query,
        headers={"Content-Type": "application/sparql-update"}
    )
    if not r.ok:
        print(f"❌ Error inserting entity: {r.status_code} – {r.text} - {insert_query}")
        return False
    return True


def process_gdf(gdf: gpd.GeoDataFrame, columns:list, TARGET_REPO:str, comment=None):
    """
    Process a GeoDataFrame and add entities to a repository based on the specified columns.
    Columns are used to find the tag used to search for that specific entity.
    The tag is used to map the entity to a specific OSM class of the ontology.
    This is done to avoid using additional paramters to know witch filter was used to create the GeoDataFrame or 
    just guessing the right class.
    For example, if the columns ['amenity'] is used, the entity will be mapped to the class corresponding to the value of 'amenity' in the
    GeoDataFrame.
    If possible it will map to point or polygon class depending on the geometry type.
    The comment is used to add a comment saying if the entity was used for isochrone or k-nearest analysis, given the fact that for now
    it doesn't exist in the ontology provided by Mayukh, a specific property to indicate this kind of information.

    Parameters
    ----------
    gdf : gpd.GeoDataFrame
        The GeoDataFrame to process
    columns : list
        The list of columns to use for mapping the entities to classes
    TARGET_REPO : str
        The ID of the repository to insert the entities into
    comment : str, optional
        A comment to add to each entity, by default None
    
    """
    # Entity mapping: OSM tag (key, value) -> ontology class
    for idx, row in gdf.iterrows():
        class_uri = None
        for col in columns:
            if col == "route" and (row[col] == "track" or row[col] == "route"):
                class_uri = f"{ROUTE}#GO_6000041"
                break
            if col in row and row[col] is not None:
                if row.geometry.geom_type == "Point":
                    class_uri = f"{BASE}#point_{row[col]}"
                    if not class_exists_in_repo(class_uri, BIG_ONTO_REPO):
                        base_name = row[col]
                    break
                else:
                    class_uri = f"{BASE}#polygon_{row[col]}"
                    if not class_exists_in_repo(class_uri, BIG_ONTO_REPO):
                        base_name = row[col]
                    break
        # Ensure class exists in repo
        # if class_uri not in added_classes:
        # print(f"Processing entity {idx} of class {class_uri}")
        if not class_exists_in_repo(class_uri, TARGET_REPO):
            turtle = get_class_hierarchy_and_axioms(class_uri)
            insert_turtle_in_repo(turtle, TARGET_REPO)
            # added_classes.add(class_uri)

        # Add entity
        # comment = "test di commento"
        add_entity(row, class_uri, TARGET_REPO, comment=comment)
    


if __name__ == "__main__":
    # construct_property("http://schema.org/geo", BIG_ONTO_REPO, TARGET_REPO)
    gdf = gpd.read_file("data/osm_points.geojson")
    #set as index the column element and column id
    gdf = gdf.set_index(["element", "id"])
    process_gdf(gdf, columns=["amenity", "highway", "historic"], TARGET_REPO=99)
    # create_repository("991", "Test Repo", persist=True, sync_delay=0)
    # delete_repository("991")
