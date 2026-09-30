"""Small helpers shared by the pipeline scripts."""
import math
from typing import Any, Optional

import yaml
from rdflib import Literal, URIRef
from rdflib.namespace import XSD

DATATYPES = {"string": XSD.string, "float": XSD.float, "int": XSD.int, "integer": XSD.integer}


def load_yaml(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def expand(curie: str, namespaces: dict) -> URIRef:
    """Turns 'prefix:local' into a full URI using the namespaces of a config file."""
    if curie.startswith("http://") or curie.startswith("https://"):
        return URIRef(curie)
    prefix, local = curie.split(":", 1)
    if prefix not in namespaces:
        raise ValueError(f"Unknown prefix '{prefix}' in '{curie}'")
    return URIRef(namespaces[prefix] + local)


def clean(value: Any) -> Optional[Any]:
    """Returns None for missing values (None, NaN, empty strings), the value otherwise."""
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    if isinstance(value, str):
        value = value.strip()
        if value == "" or value.lower() == "nan":
            return None
    return value


def typed_literal(value: Any, datatype: str) -> Optional[Literal]:
    """Builds a literal of the given datatype, or None if the value is missing or invalid."""
    value = clean(value)
    if value is None:
        return None
    try:
        if datatype == "float":
            value = float(value)
        elif datatype in ("int", "integer"):
            value = int(float(value))
        else:
            value = str(value)
    except (TypeError, ValueError):
        return None
    return Literal(value, datatype=DATATYPES[datatype])
