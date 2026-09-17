#!/usr/bin/env python3
"""Validate floating-point declarations in connector specification files.

The checker is intentionally independent from Maven and connector runtime code.
It validates the JSON contract described by 详细设计.md and fails closed for
candidate source types that are still mapped to TapNumber unless they have a
documented, source-file-specific allowlist entry.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Sequence


ROOT = Path(__file__).resolve().parents[3]
DEFAULT_ALLOWLIST = Path(__file__).with_name("floating-point-spec-allowlist.json")
DEFAULT_REGISTRY = Path(__file__).with_name("floating-point-source-registry.json")


def normalize_path(path: Path) -> str:
    return path.resolve().relative_to(ROOT).as_posix()


def is_candidate(name: str) -> bool:
    """Return whether a dataTypes key describes a floating-point candidate.

    Decimal/numeric/number are deliberately excluded: this change is not a
    decimal or general numeric-type migration.
    """
    normalized = name.strip().lower()
    return bool(
        re.search(
            r"(^|[^a-z])(float|double|real|smallfloat|half_float|scaled_float)($|[^a-z])",
            normalized,
        )
        or normalized.startswith(("float(", "float[", "double(", "double["))
        or normalized in {"float32", "float64", "binary_float", "binary_double"}
    )


def load_allowlist(path: Path) -> Dict[str, Dict[str, Mapping[str, Any]]]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    result: Dict[str, Dict[str, Mapping[str, Any]]] = {}
    for entry in raw.get("entries", []):
        source_path = str(entry["path"])
        source_type = str(entry["type"])
        if not entry.get("reason") or not entry.get("officialUrl"):
            raise ValueError(f"allowlist entry must include reason and officialUrl: {entry}")
        result.setdefault(source_path, {})[source_type] = entry
    return result


def load_registry(path: Path) -> Mapping[str, Any]:
    registry = json.loads(path.read_text(encoding="utf-8"))
    steps = registry.get("steps")
    families = registry.get("families")
    if not isinstance(steps, list) or not isinstance(families, Mapping):
        raise ValueError("registry must contain steps and families")
    return registry


def validate_registry(registry: Mapping[str, Any]) -> List[str]:
    errors: List[str] = []
    steps = registry.get("steps", [])
    families = registry.get("families", {})
    expected_ids = {f"S-{index:02d}" for index in range(1, 75)}
    actual_ids = {step.get("id") for step in steps if isinstance(step, Mapping)}
    if actual_ids != expected_ids:
        errors.append("source registry must contain exactly S-01 through S-74")
    registered_paths = set()
    for step in steps:
        if not isinstance(step, Mapping):
            errors.append("source registry step must be an object")
            continue
        family = step.get("family")
        if family not in families:
            errors.append(f"{step.get('id')}: unknown source family {family}")
        paths = step.get("paths")
        if not isinstance(paths, list) or not paths:
            errors.append(f"{step.get('id')}: paths must be a non-empty list")
            continue
        for relative_path in paths:
            if not isinstance(relative_path, str):
                errors.append(f"{step.get('id')}: registry path must be a string")
                continue
            registered_paths.add(relative_path)
            if not (ROOT / relative_path).is_file():
                errors.append(f"{step.get('id')}: missing registry path {relative_path}")
    if not registered_paths:
        errors.append("source registry contains no spec paths")
    return errors


def format_location(source_path: str, source_type: str) -> str:
    return f"{source_path} :: {source_type}"


def validate_entry(source_path: str, source_type: str, value: Mapping[str, Any], allowlist: Mapping[str, Mapping[str, Any]]) -> List[str]:
    errors: List[str] = []
    target = value.get("to")
    allowed = allowlist.get(source_type)
    resolver = value.get("mapping") == "TapFloatingPoint"

    if target == "TapFloat":
        if not resolver:
            if value.get("bit") != 32:
                errors.append("TapFloat requires bit=32")
            if value.get("storageBytes") != 4:
                errors.append("TapFloat requires storageBytes=4")
            if value.get("effectivePrecision") != 7:
                errors.append("TapFloat requires effectivePrecision=7")
        if value.get("fixed") is True:
            errors.append("TapFloat cannot have fixed=true")
    elif target == "TapDouble":
        if not resolver:
            if value.get("bit") != 64:
                errors.append("TapDouble requires bit=64")
            if value.get("storageBytes") != 8:
                errors.append("TapDouble requires storageBytes=8")
            if value.get("effectivePrecision") != 15:
                errors.append("TapDouble requires effectivePrecision=15")
        if value.get("fixed") is True:
            errors.append("TapDouble cannot have fixed=true")
    elif target == "TapNumber" and not allowed:
        errors.append("floating-point candidate cannot remain TapNumber without an explicit allowlist entry")

    if target in {"TapFloat", "TapDouble"}:
        forbidden = ("scale", "defaultScale", "preferScale", "precision", "defaultPrecision", "preferPrecision")
        present = [key for key in forbidden if key in value]
        if present:
            errors.append("binary floating-point entry must not use decimal attributes: " + ", ".join(present))
        if "mapping" in value and target == "TapFloat" and value.get("mapping") != "TapFloatingPoint":
            errors.append("parameterized floating-point entry must use mapping=TapFloatingPoint")
        if "mapping" in value and target == "TapDouble" and value.get("mapping") != "TapFloatingPoint":
            errors.append("parameterized floating-point entry must use mapping=TapFloatingPoint")
        if resolver:
            binary_range = value.get("binaryPrecision")
            default_precision = value.get("defaultBinaryPrecision")
            single_range = value.get("singlePrecision", {}).get("range")
            double_range = value.get("doublePrecision", {}).get("range")
            if not (isinstance(binary_range, list) and len(binary_range) == 2 and all(isinstance(item, int) for item in binary_range)):
                errors.append("TapFloatingPoint resolver requires an integer binaryPrecision range")
            if not isinstance(default_precision, int) or not isinstance(binary_range, list) or not (binary_range[0] <= default_precision <= binary_range[1]):
                errors.append("TapFloatingPoint resolver defaultBinaryPrecision must be inside binaryPrecision range")
            if not (isinstance(single_range, list) and len(single_range) == 2 and all(isinstance(item, int) for item in single_range)):
                errors.append("TapFloatingPoint resolver requires an integer singlePrecision.range")
            if not (isinstance(double_range, list) and len(double_range) == 2 and all(isinstance(item, int) for item in double_range)):
                errors.append("TapFloatingPoint resolver requires an integer doublePrecision.range")
            if isinstance(binary_range, list) and isinstance(single_range, list) and isinstance(double_range, list):
                if single_range[0] != binary_range[0] or double_range[1] != binary_range[1] or single_range[1] + 1 != double_range[0]:
                    errors.append("TapFloatingPoint resolver precision ranges must be contiguous and cover binaryPrecision")

    if target == "TapNumber" and allowed:
        if allowed.get("type") != source_type:
            errors.append("allowlist key/type mismatch")

    return errors


def iter_files(paths: Sequence[str], registry: Mapping[str, Any]) -> Iterable[Path]:
    if paths:
        yield from (Path(path).resolve() for path in paths)
        return
    seen = set()
    for step in registry.get("steps", []):
        for relative_path in step.get("paths", []):
            path = (ROOT / relative_path).resolve()
            if path not in seen:
                seen.add(path)
                yield path


def main(argv: Sequence[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("paths", nargs="*", help="spec JSON files; default scans both connector repositories")
    parser.add_argument("--allowlist", default=str(DEFAULT_ALLOWLIST), help="allowlist JSON path")
    parser.add_argument("--registry", default=str(DEFAULT_REGISTRY), help="canonical source registry JSON path")
    args = parser.parse_args(argv)

    allowlist = load_allowlist(Path(args.allowlist).resolve())
    registry = load_registry(Path(args.registry).resolve())
    errors: List[str] = validate_registry(registry)
    registered_paths = {
        relative_path
        for step in registry.get("steps", [])
        for relative_path in step.get("paths", [])
    }
    for source_path in allowlist:
        if source_path not in registered_paths:
            errors.append(f"allowlist path is absent from source registry: {source_path}")
    checked = 0
    for path in iter_files(args.paths, registry):
        if not path.is_file():
            errors.append(f"missing spec file: {path}")
            continue
        try:
            document = json.loads(path.read_text(encoding="utf-8"))
        except Exception as exc:  # pragma: no cover - command-line diagnostic
            errors.append(f"invalid JSON {path}: {exc}")
            continue
        source_path = normalize_path(path)
        data_types = document.get("dataTypes") or {}
        for source_type, value in data_types.items():
            if not isinstance(value, Mapping) or not is_candidate(source_type):
                continue
            checked += 1
            entry_errors = validate_entry(source_path, source_type, value, allowlist.get(source_path, {}))
            errors.extend(f"{format_location(source_path, source_type)}: {error}" for error in entry_errors)

    if errors:
        print(f"floating-point spec validation failed: {len(errors)} error(s), {checked} candidate(s) checked", file=sys.stderr)
        for error in errors:
            print(f"- {error}", file=sys.stderr)
        return 1
    print(f"floating-point spec validation passed: {checked} candidate(s) checked")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
