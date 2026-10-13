#!/usr/bin/env python3
"""Let the project's snapshot repository bypass release-only CI mirrors.

The input is a private temporary effective-settings file. Preserve its servers,
profiles and other mirrors; never print settings or credentials.
"""

import os
from pathlib import Path
import sys
import xml.etree.ElementTree as ET


def prepare(path):
    path = Path(path)
    os.chmod(path, 0o600)
    tree = ET.parse(path)
    root = tree.getroot()
    namespace = root.tag.partition("}")[0].lstrip("{") if "}" in root.tag else ""
    if namespace:
        ET.register_namespace("", namespace)
    for element in root.iter():
        if element.tag.rsplit("}", 1)[-1] != "mirrorOf":
            continue
        patterns = [part.strip() for part in (element.text or "").split(",") if part.strip()]
        # Put the exclusion first: an explicit positive repository match can
        # otherwise stop Maven's pattern evaluation before a later exclusion.
        patterns = [part for part in patterns if part != "!nexus-snapshots"]
        element.text = ",".join(["!nexus-snapshots"] + patterns)
    tree.write(path, encoding="utf-8", xml_declaration=True)


if __name__ == "__main__":
    prepare(sys.argv[1])
