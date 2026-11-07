"""Run image startup flow without Docker's marker file or real services."""
import pathlib
import subprocess
import tempfile
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[2]


class ImageEntrypointTests(unittest.TestCase):
    def test_containerd_startup_stays_in_foreground(self):
        source = (ROOT / 'build/image/docker-entrypoint.sh').read_text()
        main = '_main() {' + source.split('_main() {', 1)[1].split('\n_main "$@"', 1)[0]
        # Simulate containerd even if this regression runs inside Docker.
        main = main.replace('[[ -f /.dockerenv && $? -ne 0 ]]', '[[ false == true ]]')
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory)
            (path / 'sleep').write_text('#!/bin/bash\necho "foreground:$*"\n')
            (path / 'sleep').chmod(0o755)
            script = f'''export PATH="{directory}:$PATH"
SCRIPT_BASE_DIR="{directory}"
tm_port=3030
print_message() {{ :; }}
get_env() {{ :; }}
unzip_files() {{ :; }}
start_server() {{ echo startup-complete; }}
{main}
_main
'''
            result = subprocess.run(['bash', '-c', script], text=True, capture_output=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn('startup-complete', result.stdout)
            self.assertIn('foreground:infinity', result.stdout)


if __name__ == '__main__':
    unittest.main()
