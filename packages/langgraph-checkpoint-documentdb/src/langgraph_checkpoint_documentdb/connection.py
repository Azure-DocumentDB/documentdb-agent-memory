"""Fail-closed managed/native/Entra helpers; no credentials in errors."""
from __future__ import annotations
import ipaddress
import math
import os
import re
import time
from urllib.parse import parse_qs, quote, urlsplit
from typing import Any
from pymongo import MongoClient
from pymongo.auth_oidc import OIDCCallback, OIDCCallbackResult

AZURE_SCOPE = 'https://ossrdbms-aad.database.windows.net/.default'
_HOST = re.compile(r'^[a-z0-9][a-z0-9-]*\.(?:global\.)?mongocluster\.cosmos\.azure\.com$')


def azure_host(host: str) -> str:
    if not isinstance(host, str) or not _HOST.fullmatch(host):
        raise ValueError('expected an exact Azure DocumentDB mongocluster hostname')
    return host


def build_local_uri(username: str, password: str, host: str = '127.0.0.1', port: int = 10260, *, allow_self_signed: bool = False) -> str:
    try:
        loopback = ipaddress.ip_address(host).is_loopback
    except (ValueError, TypeError):
        loopback = host == 'localhost'
    if not loopback or type(port) is not int or not 1 <= port <= 65535:
        raise ValueError('local connections require loopback and a valid port')
    if not isinstance(username, str) or not username or not isinstance(password, str) or not password:
        raise ValueError('nonempty username/password required')
    address = f'[{host}]' if ':' in host else host
    insecure = '&tlsAllowInvalidCertificates=true' if allow_self_signed else ''
    return f'mongodb://{quote(username, safe="")}:{quote(password, safe="")}@{address}:{port}/?tls=true&directConnection=true&authMechanism=SCRAM-SHA-256{insecure}'


def build_native_uri(host: str, username: str, password: str) -> str:
    azure_host(host)
    if not isinstance(username, str) or not username or not isinstance(password, str) or not password:
        raise ValueError('nonempty username/password required')
    return f'mongodb+srv://{quote(username, safe="")}:{quote(password, safe="")}@{host}/?tls=true&authMechanism=SCRAM-SHA-256&retryWrites=false'


def validate_managed_uri(uri: str, *, allowed_host: str) -> str:
    host = azure_host(allowed_host)
    try:
        parsed = urlsplit(uri)
        options = parse_qs(parsed.query, keep_blank_values=True)
        if parsed.scheme != 'mongodb+srv' or parsed.hostname != host or parsed.port is not None or parsed.fragment or parsed.path not in ('', '/'):
            raise ValueError
        allowed = {'tls': 'true', 'authMechanism': 'SCRAM-SHA-256', 'retryWrites': 'false', 'authSource': 'admin'}
        if any(k not in allowed or v != [allowed[k]] for k, v in options.items()):
            raise ValueError
        if options.get('tls') != ['true'] or options.get('authMechanism') != ['SCRAM-SHA-256'] or not parsed.username or not parsed.password:
            raise ValueError
    except (TypeError, ValueError):
        raise ValueError('managed URI requires exact allowed Azure SRV host, native authentication and verified TLS') from None
    return uri


def managed_client(uri: str, *, allowed_host: str) -> MongoClient:
    validate_managed_uri(uri, allowed_host=allowed_host)
    return MongoClient(uri, serverSelectionTimeoutMS=20000, appname='documentdb-agent-memory')


class AzureOIDCCallback(OIDCCallback):
    def __init__(self, credential: Any):
        self.credential = credential

    def fetch(self, context: Any) -> OIDCCallbackResult:
        token = self.credential.get_token(AZURE_SCOPE)
        expires = token.expires_on - time.time()
        if not isinstance(token.token, str) or not token.token or not math.isfinite(expires) or expires <= 0:
            raise ValueError('credential returned an empty or expired Azure token')
        return OIDCCallbackResult(access_token=token.token, expires_in_seconds=expires)


def entra_client(host: str, *, credential: Any = None) -> MongoClient:
    azure_host(host)
    if credential is None:
        from azure.identity import DefaultAzureCredential
        credential = DefaultAzureCredential(managed_identity_client_id=os.environ.get('AZURE_CLIENT_ID'))
    # PyMongo 4.18.3 permits ALLOWED_HOSTS only for HUMAN callbacks, not this
    # machine flow. Validate the exact configured Azure seed above; PyMongo
    # enforces its normal SRV DNS-parent validation and verified TLS.
    return MongoClient(f'mongodb+srv://{host}/?tls=true&retryWrites=false', authMechanism='MONGODB-OIDC', authMechanismProperties={'OIDC_CALLBACK': AzureOIDCCallback(credential)}, serverSelectionTimeoutMS=20000, appname='documentdb-agent-memory')


def client_from_env() -> MongoClient:
    host = os.environ.get('DOCUMENTDB_HOST', '')
    azure_host(host)
    mode = os.environ.get('DOCUMENTDB_AUTH', '')
    if mode == 'entra':
        return entra_client(host)
    if mode == 'native':
        return managed_client(build_native_uri(host, os.environ.get('DOCUMENTDB_USERNAME', ''), os.environ.get('DOCUMENTDB_PASSWORD', '')), allowed_host=host)
    raise ValueError('DOCUMENTDB_AUTH must explicitly be native or entra')
