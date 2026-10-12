#!/usr/bin/env python3
"""Opt-in real Azure cross-language acceptance. No mocks, LLMs or database drops."""
from __future__ import annotations
import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import uuid
from urllib.parse import parse_qs, quote, unquote, urlsplit

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
PREFIX = 'readiness_memory_cross_'
COLLECTIONS = ['sessions', 'messages', 'summaries', 'checkpoints', 'checkpoint_writes']
HOST = re.compile(r'^[a-z0-9][a-z0-9-]*\.(?:global\.)?mongocluster\.cosmos\.azure\.com$')
TOKEN = re.compile(r'^[a-f0-9]{24}$')

class GateError(RuntimeError):
    """Only static, credential-free messages may cross the process boundary."""


def validate_request(request: dict, env: dict) -> None:
    if env.get('MEMORY_CROSS_LIVE') != '1':
        raise GateError('Set MEMORY_CROSS_LIVE=1 explicitly; no live work was started')
    token = request.get('token', '')
    if not TOKEN.fullmatch(token):
        raise GateError('Invalid generated run token')
    if request.get('database') not in {f'{PREFIX}{token}_{n}' for n in ('pyts', 'tspy')}:
        raise GateError('Database must be an exact generated readiness_memory_cross_* name')
    if request.get('namespace') not in {f'cross_{token}_{n}' for n in ('a', 'b', 'legacy')}:
        raise GateError('Namespace must be an exact generated run namespace')
    if request.get('thread') not in {'collision', 'resume', 'neighbor'}:
        raise GateError('Invalid isolated thread')


def normalize_environment(source: dict) -> dict:
    """Translate either package's env without reading any credential files."""
    env = dict(source)
    py_mode, ts_mode = env.get('DOCUMENTDB_AUTH'), env.get('MONGO_AUTH_MODE')
    if py_mode not in (None, '', 'native', 'entra') or ts_mode not in (None, '', 'connection-string', 'oidc'):
        raise GateError('Only managed Azure native or OIDC authentication is supported')
    if py_mode and ts_mode and (py_mode == 'native') != (ts_mode == 'connection-string'):
        raise GateError('Conflicting Python and TypeScript authentication modes')
    mode = py_mode or {'connection-string': 'native', 'oidc': 'entra'}.get(ts_mode)
    if mode not in ('native', 'entra'):
        raise GateError('Explicit DOCUMENTDB_AUTH or MONGO_AUTH_MODE is required')
    if env.get('DOCUMENTDB_URI'):
        raise GateError('Local/documentdb URI override is forbidden')
    host = env.get('DOCUMENTDB_HOST') or env.get('MONGO_HOST')
    if env.get('DOCUMENTDB_HOST') and env.get('MONGO_HOST') and env['DOCUMENTDB_HOST'] != env['MONGO_HOST']:
        raise GateError('Python and TypeScript host mismatch')
    if mode == 'native':
        username, password = env.get('DOCUMENTDB_USERNAME'), env.get('DOCUMENTDB_PASSWORD')
        uri = env.get('MONGO_CONNECTION_STRING')
        if uri:
            try:
                parsed = urlsplit(uri)
                opts = parse_qs(parsed.query, keep_blank_values=True)
                allowed = {'tls': 'true', 'authMechanism': 'SCRAM-SHA-256', 'retryWrites': 'false', 'authSource': 'admin'}
                if parsed.scheme != 'mongodb+srv' or parsed.port or parsed.fragment or parsed.path not in ('', '/') or any(k not in allowed or v != [allowed[k]] for k, v in opts.items()):
                    raise ValueError
                if not parsed.username or not parsed.password:
                    raise ValueError
                uh, un, pw = parsed.hostname, unquote(parsed.username), unquote(parsed.password)
                if (host and host != uh) or (username and username != un) or (password and password != pw):
                    raise ValueError
                host, username, password = uh, un, pw
            except (ValueError, TypeError):
                raise GateError('Native URI is unsafe or conflicts with package environment') from None
        if not username or not password or env.get('MONGO_OIDC_USERNAME'):
            raise GateError('Native username/password required; OIDC username must be unset')
        env.update(DOCUMENTDB_USERNAME=username, DOCUMENTDB_PASSWORD=password)
        if not host or not HOST.fullmatch(host):
            raise GateError('Exact managed Azure mongocluster hostname required')
        env['MONGO_CONNECTION_STRING'] = f'mongodb+srv://{quote(username, safe="")}:{quote(password, safe="")}@{host}/?tls=true&authMechanism=SCRAM-SHA-256&retryWrites=false'
        env['MONGO_AUTH_MODE'] = 'connection-string'
    else:
        if env.get('MONGO_CONNECTION_STRING') or env.get('DOCUMENTDB_USERNAME') or env.get('DOCUMENTDB_PASSWORD'):
            raise GateError('OIDC mode forbids supplied native credentials/URI')
        env['MONGO_AUTH_MODE'] = 'oidc'
    if not host or not HOST.fullmatch(host):
        raise GateError('Exact managed Azure mongocluster hostname required')
    env.update(DOCUMENTDB_HOST=host, MONGO_HOST=host, DOCUMENTDB_AUTH=mode)
    # Never rely on a shell or inherited Python path to resolve local package source.
    env['PYTHONPATH'] = str(ROOT / 'packages/langgraph-checkpoint-documentdb/src')
    env['PYTHONDONTWRITEBYTECODE'] = '1'
    return env


def run_worker(language: str, request: dict, env: dict, python: str, timeout: int = 180) -> dict:
    validate_request(request, env)
    command = [python, str(HERE / 'python_worker.py')] if language == 'python' else ['node', str(HERE / 'node_worker.mjs')]
    try:
        completed = subprocess.run(command, input=json.dumps(request), text=True, capture_output=True, env=env, timeout=timeout, check=False)
    except subprocess.TimeoutExpired:
        raise GateError(f'{language}/{request["action"]}: timeout; child terminated; cleanup will be attempted') from None
    # Never relay driver exceptions, stdout/stderr or command environments: they can contain credentials.
    if completed.returncode:
        kind = 'worker failure'
        try:
            candidate = json.loads(completed.stdout).get('errorType', '')
            if re.fullmatch(r'[A-Za-z][A-Za-z0-9_]{0,79}', candidate):
                kind = candidate
        except (ValueError, AttributeError):
            pass
        raise GateError(f'{language}/{request["action"]}: {kind}; exit {completed.returncode}; raw output suppressed')
    try:
        result = json.loads(completed.stdout)
    except ValueError:
        raise GateError(f'{language}/{request["action"]}: invalid worker response; raw output suppressed') from None
    if result.get('ok') is not True or result.get('action') != request['action'] or not isinstance(result.get('pid'), int):
        raise GateError('Worker response contract failed')
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--python', default=os.environ.get('MEMORY_CROSS_PYTHON', str(ROOT.parent / 'python-evidence/venv/bin/python')), help='Existing project venv; never installs dependencies')
    parser.add_argument('--output', type=Path, help='Optional credential-free JSON evidence file')
    args = parser.parse_args()
    token = uuid.uuid4().hex[:24]
    request = {'token': token, 'database': f'{PREFIX}{token}_pyts', 'namespace': f'cross_{token}_a', 'thread': 'collision'}
    # The explicit live guard precedes dependency lookup, subprocesses and all connections.
    validate_request(request, os.environ)
    env = normalize_environment(os.environ)
    if not Path(args.python).is_file() or not (ROOT / 'packages/vercel-ai-memory-documentdb/dist/index.js').is_file():
        raise GateError('Build TypeScript and provide existing Python venv before live execution')
    events, touched = [], []
    def call(lang, action, **overrides):
        req = {**request, **overrides, 'action': action}
        result = run_worker(lang, req, env, args.python)
        events.append({'language': lang, 'database': req['database'], 'namespace': req['namespace'], **result})
        print(json.dumps({'stage': action, 'language': lang, 'status': 'pass', 'pid': result['pid']}), flush=True)
        return result
    failure = None
    cleanup_failures = []
    try:
        for order, first, second in [('pyts', 'python', 'node'), ('tspy', 'node', 'python')]:
            request['database'] = f'{PREFIX}{token}_{order}'
            touched.append(request['database'])
            a = call(first, 'setup-fresh')
            b = call(second, 'setup')
            c = call(first, 'setup')
            expected_catalog = json.loads((HERE / 'schema_fixture.json').read_text())
            if a['catalog'] != expected_catalog or a['catalog'] != b['catalog'] or b['catalog'] != c['catalog']:
                raise GateError('Shared schema fixture/catalog mismatch or non-idempotent setup')
            # Real graphs write genuine checkpoint and pending-write payloads first.
            for ns in ('a', 'b'):
                namespace = f'cross_{token}_{ns}'
                call('python', 'seed', namespace=namespace)
                call('node', 'append', namespace=namespace)
                call('node', 'read', namespace=namespace)
                call('python', 'inspect', namespace=namespace)
            # Same namespace, different thread: both namespace AND thread scoping are tested.
            call('python', 'seed', thread='neighbor')
            call('node', 'append', thread='neighbor')
            before_b = call('python', 'inspect', namespace=f'cross_{token}_b')['counts']
            before_neighbor = call('python', 'inspect', thread='neighbor')['counts']
            deleter = 'node' if order == 'pyts' else 'python'
            deleted = call(deleter, 'delete')
            if set(deleted['counts']) != set(COLLECTIONS) or not all(v > 0 for v in deleted['counts'].values()):
                raise GateError('Lifecycle deletion did not acknowledge all five collections')
            call('python', 'absent')
            after_b = call('python', 'inspect', namespace=f'cross_{token}_b')['counts']
            after_neighbor = call('python', 'inspect', thread='neighbor')['counts']
            if before_b != after_b or before_neighbor != after_neighbor:
                raise GateError('Lifecycle deletion modified non-target scope')
        request['database'] = f'{PREFIX}{token}_pyts'
        saved = call('python', 'graph-save', thread='resume')
        restored = call('python', 'graph-resume', thread='resume', checkpoint_id=saved['checkpoint_id'])
        if saved['pid'] == restored['pid'] or restored['restored_checkpoint_id'] != saved['checkpoint_id']:
            raise GateError('Fresh-process checkpoint identity proof failed')
        # Inject only our exact scoped malformed legacy fixture; neither adapter may mutate catalogs.
        call('python', 'legacy-insert', namespace=f'cross_{token}_legacy')
        call('python', 'legacy-reject', namespace=f'cross_{token}_legacy')
        call('node', 'legacy-reject', namespace=f'cross_{token}_legacy')
    except Exception as exc:
        failure = str(exc) if isinstance(exc, GateError) else type(exc).__name__
    finally:
        for database in touched:
            try:
                call('python', 'cleanup', database=database)
            except Exception as exc:
                cleanup_failures.append({'database': database, 'error': str(exc) if isinstance(exc, GateError) else type(exc).__name__})
    report = {'gate': 'cross-language-real-azure', 'status': 'pass' if not failure and not cleanup_failures else 'fail', 'run_token': token, 'databases': touched, 'auth': env['DOCUMENTDB_AUTH'], 'dimensions': 3, 'ivf_num_lists': 1, 'quiescent_deletion_only': True, 'real_llm': False, 'failure': failure, 'cleanup_failures': cleanup_failures, 'events': events}
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps({'gate': report['gate'], 'status': report['status'], 'failure': failure, 'cleanup_failures': cleanup_failures}))
    return 0 if report['status'] == 'pass' else 1

if __name__ == '__main__':
    try:
        sys.exit(main())
    except GateError as exc:
        print(json.dumps({'status': 'blocked', 'error': str(exc)}))
        sys.exit(2)
