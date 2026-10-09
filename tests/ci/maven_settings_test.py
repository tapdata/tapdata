import importlib.util
from pathlib import Path
import stat
import tempfile
import unittest
import xml.etree.ElementTree as ET


ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location("ci_settings", ROOT / "build/prepare-maven-ci-settings.py")
SETTINGS = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(SETTINGS)


class MavenSettingsTest(unittest.TestCase):
    def test_preserves_credentials_and_mirrors_while_excluding_snapshots_first(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "settings.xml"
            path.write_text('''<settings xmlns="http://maven.apache.org/SETTINGS/1.2.0">
              <servers><server><id>private</id><username>fixture-user</username>
                <password>{fixture-encrypted-password}</password></server></servers>
              <mirrors>
                <mirror><id>public</id><url>https://example.test/public</url><mirrorOf>*</mirrorOf></mirror>
                <mirror><id>explicit</id><url>https://example.test/other</url><mirrorOf>nexus-snapshots,central</mirrorOf></mirror>
              </mirrors><activeProfiles><activeProfile>private-profile</activeProfile></activeProfiles>
            </settings>''')
            path.chmod(0o644)

            SETTINGS.prepare(path)

            root = ET.parse(path).getroot()
            ns = {"m": "http://maven.apache.org/SETTINGS/1.2.0"}
            self.assertEqual("{fixture-encrypted-password}", root.findtext("m:servers/m:server/m:password", namespaces=ns))
            self.assertEqual("private-profile", root.findtext("m:activeProfiles/m:activeProfile", namespaces=ns))
            mirrors = root.findall("m:mirrors/m:mirror", ns)
            self.assertEqual("https://example.test/public", mirrors[0].findtext("m:url", namespaces=ns))
            self.assertEqual("!nexus-snapshots,*", mirrors[0].findtext("m:mirrorOf", namespaces=ns))
            self.assertEqual("!nexus-snapshots,nexus-snapshots,central", mirrors[1].findtext("m:mirrorOf", namespaces=ns))
            self.assertEqual(0o600, stat.S_IMODE(path.stat().st_mode))

    def test_existing_exclusion_is_idempotent_without_a_namespace(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "settings.xml"
            path.write_text("<settings><mirrors><mirror><mirrorOf>*,!nexus-snapshots</mirrorOf></mirror></mirrors></settings>")
            SETTINGS.prepare(path)
            first = path.read_bytes()
            SETTINGS.prepare(path)
            self.assertEqual(first, path.read_bytes())
            self.assertEqual("!nexus-snapshots,*", ET.parse(path).findtext("mirrors/mirror/mirrorOf"))


if __name__ == "__main__":
    unittest.main()
