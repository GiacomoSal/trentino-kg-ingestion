"""Stage 3: entity unification.

Finds the entities of two source KGs that describe the same real-world thing and
links them with owl:sameAs. Both entities stay in the graph with their original
URI and type, so provenance is preserved.

1. Type filter (always on): two entities are candidates only if their types are
   compatible in the unifying ontology plus the teleontology, i.e. one type is
   equal to or a subclass of the other.
2. Primitives (each can be switched on or off in unification.yaml):
   - name: normalized Levenshtein similarity of the names >= min_similarity
   - coordinates: distance between the two points <= max_distance_m
   Every enabled primitive must hold.
3. If one_to_one is set, each entity keeps only its best candidate.

The name and coordinate properties of each source are found through the
teleontology: every property equivalent to those of the unifying ontology.
"""
import csv
import math
import re
import sys
import unicodedata
from collections import defaultdict
from typing import Dict, List, Optional, Set

from rdflib import Graph, URIRef
from rdflib.namespace import OWL, RDF, RDFS

from kg_utils import expand, load_yaml

CONFIG = "unification.yaml"

try:  # optional, much faster; the pure Python version gives the same results
    from rapidfuzz.distance import Levenshtein as _rf_levenshtein
except ImportError:
    _rf_levenshtein = None


# ---------------------------------------------------------------- primitives

def levenshtein(a: str, b: str) -> int:
    if _rf_levenshtein is not None:
        return _rf_levenshtein.distance(a, b)
    if len(a) < len(b):
        a, b = b, a
    previous = list(range(len(b) + 1))
    for i, ca in enumerate(a, 1):
        current = [i]
        for j, cb in enumerate(b, 1):
            current.append(min(previous[j] + 1, current[j - 1] + 1, previous[j - 1] + (ca != cb)))
        previous = current
    return previous[-1]


NORMALIZATION: dict = {}  # filled from unification.yaml in main()


def normalize(name: str) -> str:
    """Lowercase and collapse spaces; optionally drop accents, punctuation and generic words."""
    name = name.lower()
    if NORMALIZATION.get("strip_accents"):
        name = "".join(c for c in unicodedata.normalize("NFKD", name) if not unicodedata.combining(c))
    if NORMALIZATION.get("strip_punctuation"):
        name = re.sub(r"[^\w\s]", " ", name)
    ignore = set(NORMALIZATION.get("ignore_words") or [])
    kept = [w for w in name.split() if w not in ignore]
    # a name made only of generic words ("Hotel Bar") is kept as it is
    return " ".join(kept) if kept else " ".join(name.split())


def name_similarity(a: str, b: str) -> float:
    """Normalized Levenshtein similarity of two already normalized names."""
    longest = max(len(a), len(b))
    return 1.0 if longest == 0 else 1.0 - levenshtein(a, b) / longest


def distance_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance (haversine) in metres."""
    r = 6371000.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = p2 - p1, math.radians(lon2 - lon1)
    h = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(h))


# ---------------------------------------------------------------- schema

class Schema:
    """Class hierarchy and property equivalences of unifying ontology + teleontology."""

    def __init__(self, graph: Graph):
        self.parents: Dict[URIRef, Set[URIRef]] = defaultdict(set)
        for c, p in graph.subject_objects(RDFS.subClassOf):
            if isinstance(p, URIRef):
                self.parents[c].add(p)
        for a, b in graph.subject_objects(OWL.equivalentClass):
            if isinstance(b, URIRef):
                self.parents[a].add(b)
                self.parents[b].add(a)
        self.equivalent_props: Dict[URIRef, Set[URIRef]] = defaultdict(set)
        for a, b in graph.subject_objects(OWL.equivalentProperty):
            self.equivalent_props[a].add(b)
            self.equivalent_props[b].add(a)
        self._ancestors: Dict[URIRef, Set[URIRef]] = {}

    def ancestors(self, cls: URIRef) -> Set[URIRef]:
        """The class itself and all its superclasses."""
        if cls not in self._ancestors:
            seen, todo = {cls}, [cls]
            while todo:
                for p in self.parents.get(todo.pop(), ()):
                    if p not in seen:
                        seen.add(p)
                        todo.append(p)
            self._ancestors[cls] = seen
        return self._ancestors[cls]

    def compatible(self, a: URIRef, b: URIRef) -> bool:
        return b in self.ancestors(a) or a in self.ancestors(b)

    def property_family(self, prop: URIRef) -> Set[URIRef]:
        seen, todo = {prop}, [prop]
        while todo:
            for p in self.equivalent_props.get(todo.pop(), ()):
                if p not in seen:
                    seen.add(p)
                    todo.append(p)
        return seen


# ---------------------------------------------------------------- entities

class Entity:
    def __init__(self, uri: URIRef):
        self.uri = uri
        self.types: Set[URIRef] = set()
        self.name: Optional[str] = None
        self.lat: Optional[float] = None
        self.lon: Optional[float] = None


def first_value(graph: Graph, subject, props: Set[URIRef]):
    for p in props:
        for o in graph.objects(subject, p):
            return o
    return None


def read_entities(graph: Graph, name_props, lat_props, lon_props) -> List[Entity]:
    entities = []
    for subject in set(graph.subjects(RDF.type, None)):
        if not isinstance(subject, URIRef):
            continue
        e = Entity(subject)
        e.types = {t for t in graph.objects(subject, RDF.type) if isinstance(t, URIRef)}
        name = first_value(graph, subject, name_props)
        e.name = str(name) if name is not None else None
        lat, lon = first_value(graph, subject, lat_props), first_value(graph, subject, lon_props)
        try:
            e.lat, e.lon = float(lat), float(lon)
        except (TypeError, ValueError):
            pass
        entities.append(e)
    return entities


# ---------------------------------------------------------------- main

def main() -> None:
    # an alternative config file can be passed on the command line (for experiments)
    cfg = load_yaml(sys.argv[1] if len(sys.argv) > 1 else CONFIG)
    ns = cfg["namespaces"]
    prim = cfg["primitives"]
    use_name = prim["name"]["enabled"]
    use_coords = prim["coordinates"]["enabled"]
    min_sim = float(prim["name"]["min_similarity"])
    max_dist = float(prim["coordinates"]["max_distance_m"])
    NORMALIZATION.update(prim["name"].get("normalization") or {})

    schema_graph = Graph()
    schema_graph.parse(cfg["unified_ontology"])
    schema_graph.parse(cfg["teleontology"])
    schema = Schema(schema_graph)
    name_props = schema.property_family(expand(cfg["name_property"], ns))
    lat_props = schema.property_family(expand(cfg["latitude_property"], ns))
    lon_props = schema.property_family(expand(cfg["longitude_property"], ns))

    graphs = {}
    for key, path in cfg["sources"].items():
        g = Graph()
        g.parse(path)
        graphs[key] = g
    left = read_entities(graphs[cfg["link"]["from"]], name_props, lat_props, lon_props)
    right = read_entities(graphs[cfg["link"]["to"]], name_props, lat_props, lon_props)

    enabled = [p for p, on in (("name", use_name), ("coordinates", use_coords)) if on]
    print(f"Entities: {cfg['link']['from']} {len(left)}, {cfg['link']['to']} {len(right)}")
    print(f"Filters: type (always) + {', '.join(enabled) if enabled else 'no primitive'}")
    if not enabled:
        print("[WARNING] no primitive enabled: every type-compatible pair would be linked")

    # group the right side by type to avoid comparing incompatible entities
    right_by_type: Dict[URIRef, List[Entity]] = defaultdict(list)
    for e in right:
        for t in e.types:
            right_by_type[t].append(e)

    candidates = []
    type_pairs = 0
    for a in left:
        seen = set()
        for ta in a.types:
            for tb, group in right_by_type.items():
                if not schema.compatible(ta, tb):
                    continue
                for b in group:
                    if b.uri in seen:
                        continue
                    seen.add(b.uri)
                    type_pairs += 1
                    dist = sim = None
                    if use_coords:
                        if None in (a.lat, a.lon, b.lat, b.lon):
                            continue
                        dist = distance_m(a.lat, a.lon, b.lat, b.lon)
                        if dist > max_dist:
                            continue
                    if use_name:
                        if not a.name or not b.name:
                            continue
                        na, nb = normalize(a.name), normalize(b.name)
                        # cheap bound: the length difference alone can exclude the pair
                        if 1 - abs(len(na) - len(nb)) / max(len(na), len(nb), 1) < min_sim:
                            continue
                        sim = name_similarity(na, nb)
                        if sim < min_sim:
                            continue
                    # higher is better: name similarity, then closeness
                    score = (sim if sim is not None else 0.0) + (1 - dist / max_dist if dist is not None else 0.0)
                    candidates.append((score, a, b, sim, dist))

    candidates.sort(key=lambda c: -c[0])
    links, used_left, used_right = [], set(), set()
    for score, a, b, sim, dist in candidates:
        if cfg.get("one_to_one", True) and (a.uri in used_left or b.uri in used_right):
            continue
        used_left.add(a.uri)
        used_right.add(b.uri)
        links.append((a, b, sim, dist))

    unified = Graph()
    for g in graphs.values():
        unified += g
        for prefix, uri in g.namespaces():
            unified.bind(prefix, uri, replace=True)
    unified.bind("owl", OWL)
    for a, b, _, _ in links:
        unified.add((a.uri, OWL.sameAs, b.uri))
    unified.serialize(destination=cfg["output"], format="turtle", encoding="utf-8")

    with open(cfg["report"], "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["from_uri", "from_name", "to_uri", "to_name", "name_similarity", "distance_m"])
        for a, b, sim, dist in sorted(links, key=lambda l: l[0].name or ""):
            w.writerow([a.uri, a.name, b.uri, b.name,
                        "" if sim is None else f"{sim:.2f}", "" if dist is None else f"{dist:.1f}"])

    print(f"Type-compatible pairs: {type_pairs}, passing the primitives: {len(candidates)}")
    print(f"owl:sameAs links: {len(links)} -> {cfg['output']} (details in {cfg['report']})")


if __name__ == "__main__":
    main()
