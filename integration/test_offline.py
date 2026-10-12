"""Offline orchestration/security/syntax checks. NOT a database acceptance pass."""
import ast
import json
import os
from pathlib import Path
import subprocess
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
import coordinator as c

HOST = 'fixture.global.mongocluster.cosmos.azure.com'
TOKEN = 'a' * 24
REQUEST = {'token': TOKEN, 'database': f'{c.PREFIX}{TOKEN}_pyts', 'namespace': f'cross_{TOKEN}_a', 'thread': 'collision', 'action': 'setup'}

class OfflineTests(unittest.TestCase):
    def test_guard_requires_exact_opt_in(self):
        for val in ('', 'true', '0', 'yes'):
            with self.subTest(val=val), self.assertRaises(c.GateError):
                c.validate_request(REQUEST, {'MEMORY_CROSS_LIVE': val})
        c.validate_request(REQUEST, {'MEMORY_CROSS_LIVE': '1'})

    def test_exact_random_scope_only(self):
        for key, value in [('database', 'production'), ('database', 'readiness_memory_cross_foo'), ('namespace', 'default'), ('namespace', f'cross_{TOKEN}_a-other'), ('thread', '*'), ('token', 'x' * 24)]:
            with self.subTest(key=key, value=value), self.assertRaises(c.GateError):
                c.validate_request({**REQUEST, key: value}, {'MEMORY_CROSS_LIVE': '1'})

    def test_no_subprocess_before_guard(self):
        with patch.object(c.subprocess, 'run') as run:
            with self.assertRaises(c.GateError):
                c.run_worker('python', REQUEST, {}, sys.executable)
            run.assert_not_called()

    def test_translate_native_python_environment(self):
        env = c.normalize_environment({'DOCUMENTDB_AUTH': 'native', 'DOCUMENTDB_HOST': HOST, 'DOCUMENTDB_USERNAME': 'u@x', 'DOCUMENTDB_PASSWORD': 'fake:/pw'})
        self.assertEqual(env['MONGO_AUTH_MODE'], 'connection-string')
        self.assertIn('u%40x:fake%3A%2Fpw@', env['MONGO_CONNECTION_STRING'])
        self.assertEqual(env['MONGO_HOST'], HOST)

    def test_translate_native_typescript_environment(self):
        env = c.normalize_environment({'MONGO_AUTH_MODE': 'connection-string', 'MONGO_CONNECTION_STRING': f'mongodb+srv://u:p@{HOST}/?tls=true&authMechanism=SCRAM-SHA-256&retryWrites=false'})
        self.assertEqual((env['DOCUMENTDB_AUTH'], env['DOCUMENTDB_HOST'], env['DOCUMENTDB_USERNAME']), ('native', HOST, 'u'))

    def test_native_uri_rejects_insecure_options(self):
        for suffix in ('tls=false', 'tlsAllowInvalidCertificates=true', 'retryWrites=true'):
            with self.subTest(suffix=suffix), self.assertRaises(c.GateError):
                c.normalize_environment({'MONGO_AUTH_MODE': 'connection-string', 'MONGO_CONNECTION_STRING': f'mongodb+srv://u:p@{HOST}/?{suffix}'})

    def test_translate_oidc_and_reject_mixed_modes(self):
        for source in ({'DOCUMENTDB_AUTH': 'entra', 'DOCUMENTDB_HOST': HOST}, {'MONGO_AUTH_MODE': 'oidc', 'MONGO_HOST': HOST}):
            env = c.normalize_environment(source)
            self.assertEqual((env['DOCUMENTDB_AUTH'], env['MONGO_AUTH_MODE']), ('entra', 'oidc'))
            self.assertNotIn('MONGO_CONNECTION_STRING', env)
        for source in (
            {'DOCUMENTDB_AUTH': 'entra', 'MONGO_AUTH_MODE': 'connection-string'},
            {'DOCUMENTDB_AUTH': 'entra', 'DOCUMENTDB_HOST': HOST, 'DOCUMENTDB_PASSWORD': 'fake'},
            {'DOCUMENTDB_AUTH': 'entra', 'DOCUMENTDB_HOST': HOST, 'MONGO_HOST': 'other.global.mongocluster.cosmos.azure.com'},
            {'DOCUMENTDB_AUTH': 'entra', 'DOCUMENTDB_HOST': 'localhost'},
        ):
            with self.subTest(source=source), self.assertRaises(c.GateError):
                c.normalize_environment(source)

    def test_subprocess_failure_does_not_echo_sensitive_output(self):
        result = subprocess.CompletedProcess([], 1, 'mongodb+srv://secret:secret@host', 'token-secret')
        with patch.object(c.subprocess, 'run', return_value=result), self.assertRaises(c.GateError) as error:
            c.run_worker('python', REQUEST, {'MEMORY_CROSS_LIVE': '1'}, sys.executable)
        self.assertNotIn('secret', str(error.exception))
        self.assertNotIn('token', str(error.exception))

    def test_shared_schema_fixture(self):
        fixture = json.loads((c.HERE / 'schema_fixture.json').read_text())
        self.assertEqual(set(fixture), set(c.COLLECTIONS))
        self.assertEqual(sum(map(len, fixture.values())), 8)
        for name, indexes in fixture.items():
            unique = [i for i in indexes if i['unique']]
            self.assertEqual(len(unique), 0 if name == 'summaries' else 1)
            self.assertTrue(all(i['key'][0] == ['namespace', 1] for i in unique))
        vector = next(i for i in fixture['summaries'] if i['vector'])
        self.assertEqual(vector['vector'], {'kind': 'vector-ivf', 'similarity': 'COS', 'dimensions': 3, 'numLists': 1})

    def test_python_syntax(self):
        for path in c.HERE.glob('*.py'):
            ast.parse(path.read_text(), filename=str(path))

    def test_node_syntax(self):
        result = subprocess.run(['node', '--check', str(c.HERE / 'node_worker.mjs')], capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_direct_workers_fail_closed_without_live_flag(self):
        env = {k: v for k, v in os.environ.items() if k != 'MEMORY_CROSS_LIVE'}
        for command in ([sys.executable, str(c.HERE / 'python_worker.py')], ['node', str(c.HERE / 'node_worker.mjs')]):
            with self.subTest(command=command):
                result = subprocess.run(command, input=json.dumps(REQUEST), env=env, capture_output=True, text=True)
                self.assertNotEqual(result.returncode, 0)
                self.assertFalse(json.loads(result.stdout)['ok'])

    def test_cli_fail_closed_without_config(self):
        env = {k: v for k, v in os.environ.items() if k != 'MEMORY_CROSS_LIVE'}
        result = subprocess.run([sys.executable, str(c.HERE / 'coordinator.py')], env=env, capture_output=True, text=True)
        self.assertEqual(result.returncode, 2)
        self.assertEqual(json.loads(result.stdout)['status'], 'blocked')

if __name__ == '__main__':
    unittest.main()
