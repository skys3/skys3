"""The result schema (tests/e2e/perf/result.schema.json) and a validator
for the part of JSON Schema it uses, since the harness takes nothing beyond
the standard library: `type`, `const`, `enum`, `required`, `properties`,
`additionalProperties`, `items`, `minimum`, and local `$ref`s."""

from __future__ import annotations

import json
import os

SCHEMA_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                           "result.schema.json")

_TYPES = {
    "object": lambda v: isinstance(v, dict),
    "array": lambda v: isinstance(v, list),
    "string": lambda v: isinstance(v, str),
    "boolean": lambda v: isinstance(v, bool),
    "null": lambda v: v is None,
    "integer": lambda v: isinstance(v, int) and not isinstance(v, bool),
    "number": lambda v: isinstance(v, (int, float)) and not isinstance(v, bool),
}


def load() -> dict:
    with open(SCHEMA_PATH) as f:
        return json.load(f)


def validate(instance: object, schema: dict | None = None) -> list[str]:
    """Every way `instance` breaks the schema, each as `path: problem`;
    empty if it conforms."""
    root = schema if schema is not None else load()
    errors: list[str] = []
    _check(instance, root, root, "$", errors)
    return errors


def _check(value: object, schema: dict, root: dict, path: str, errors: list[str]) -> None:
    if "$ref" in schema:
        ref = schema["$ref"]
        if not ref.startswith("#/"):
            raise ValueError(f"only local references are supported, not {ref}")
        target = root
        for part in ref[2:].split("/"):
            target = target[part]
        _check(value, target, root, path, errors)
        return
    if "type" in schema:
        kinds = schema["type"] if isinstance(schema["type"], list) else [schema["type"]]
        if not any(_TYPES[kind](value) for kind in kinds):
            errors.append(f"{path}: {type(value).__name__} is not {' or '.join(kinds)}")
            return
    if "const" in schema and value != schema["const"]:
        errors.append(f"{path}: {value!r} is not {schema['const']!r}")
    if "enum" in schema and value not in schema["enum"]:
        errors.append(f"{path}: {value!r} is not one of {schema['enum']}")
    if "minimum" in schema and _TYPES["number"](value) and value < schema["minimum"]:
        errors.append(f"{path}: {value} is below {schema['minimum']}")
    if isinstance(value, dict):
        for name in schema.get("required", []):
            if name not in value:
                errors.append(f"{path}: {name} is missing")
        properties = schema.get("properties", {})
        extra = schema.get("additionalProperties", True)
        for name, item in value.items():
            if name in properties:
                _check(item, properties[name], root, f"{path}.{name}", errors)
            elif extra is False:
                errors.append(f"{path}: {name} is not allowed")
            elif isinstance(extra, dict):
                _check(item, extra, root, f"{path}.{name}", errors)
    if isinstance(value, list) and "items" in schema:
        for i, item in enumerate(value):
            _check(item, schema["items"], root, f"{path}[{i}]", errors)
