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
            if value.get("binaryPrecision") != [1, 53]:
                errors.append("TapFloatingPoint resolver requires binaryPrecision=[1,53]")
            if value.get("defaultBinaryPrecision") != 53:
                errors.append("TapFloatingPoint resolver requires defaultBinaryPrecision=53")
            if value.get("singlePrecision", {}).get("range") != [1, 24]:
                errors.append("TapFloatingPoint resolver requires singlePrecision.range=[1,24]")
            if value.get("doublePrecision", {}).get("range") != [25, 53]:
                errors.append("TapFloatingPoint resolver requires doublePrecision.range=[25,53]")

    if target == "TapNumber" and allowed:
        if allowed.get("type") != source_type:
            errors.append("allowlist key/type mismatch")

    return errors


def iter_files(paths: Sequence[str]) -> Iterable[Path]:
    if paths:
        yield from (Path(path).resolve() for path in paths)
        return
    for repository in (ROOT / "tapdata-connectors", ROOT / "tapdata-connectors-enterprise"):
        for path in repository.glob("**/src/main/resources/*.json"):
            yield path


def main(argv: Sequence[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("paths", nargs="*", help="spec JSON files; default scans both connector repositories")
    parser.add_argument("--allowlist", default=str(DEFAULT_ALLOWLIST), help="allowlist JSON path")
    args = parser.parse_args(argv)

    allowlist = load_allowlist(Path(args.allowlist).resolve())
    errors: List[str] = []
    checked = 0
    for path in iter_files(args.paths):
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
