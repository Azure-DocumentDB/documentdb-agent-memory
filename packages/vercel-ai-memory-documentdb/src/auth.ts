import { MongoClient, type MongoClientOptions, type OIDCCallbackParams, type OIDCResponse } from "mongodb";
const AZURE_HOST = /^[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\.(?:global\.)?mongocluster\.cosmos\.azure\.com$/;
export const AZURE_SCOPE = "https://ossrdbms-aad.database.windows.net/.default";
function requireValue(value: unknown, reason: string): asserts value { if (!value) throw new Error(reason); }
export function buildLocalUri(username: string, password: string, host = "127.0.0.1", port = 10260): string {
  requireValue(["localhost", "127.0.0.1", "[::1]"].includes(host), "Local URI only supports loopback hosts");
  requireValue(Number.isInteger(port) && port > 0 && port <= 65535, "Invalid local port");
  requireValue(username.trim() && password.trim(), "Native credentials required");
  return `mongodb://${encodeURIComponent(username)}:${encodeURIComponent(password)}@${host}:${port}/?tls=true&tlsAllowInvalidCertificates=true&directConnection=true&authMechanism=SCRAM-SHA-256&retryWrites=false`;
}
export function validateURI(raw: string): { host: string; local: boolean } {
  let uri: URL;
  try { uri = new URL(raw); } catch { throw new Error("Invalid connection URI"); }
  const local = uri.protocol === "mongodb:" && ["localhost", "127.0.0.1", "[::1]"].includes(uri.hostname);
  requireValue(local || (uri.protocol === "mongodb+srv:" && AZURE_HOST.test(uri.hostname) && !uri.port), "Unsafe connection endpoint");
  requireValue(!uri.hash && !raw.includes("#") && (!uri.pathname || uri.pathname === "/"), "Connection URI must not include database or fragment");
  try { requireValue(decodeURIComponent(uri.username).trim() && decodeURIComponent(uri.password).trim(), "Native credentials required"); } catch { throw new Error("Invalid native credentials"); }
  const seen = new Set<string>();
  for (const [key, value] of uri.searchParams) {
    const k = key.toLowerCase(), v = value.toLowerCase();
    requireValue(!seen.has(k), "Duplicate URI option"); seen.add(k);
    const allowed = (["tls", "ssl"].includes(k) && v === "true") ||
      (["retrywrites", "tlsinsecure", "tlsallowinvalidhostnames"].includes(k) && v === "false") ||
      (k === "tlsallowinvalidcertificates" && (v === "false" || (local && v === "true"))) ||
      (k === "directconnection" && local && v === "true") ||
      (k === "authmechanism" && ["SCRAM-SHA-256", "SCRAM-SHA-1"].includes(value)) ||
      (k === "authsource" && value === "admin") ||
      (k === "appname" && /^[A-Za-z0-9_-]{1,80}$/.test(value));
    requireValue(allowed, "Unsupported or unsafe URI option");
  }
  return { host: uri.hostname, local };
}
const COMMON: MongoClientOptions = { tls: true, tlsAllowInvalidHostnames: false, retryWrites: false, serverSelectionTimeoutMS: 20000, connectTimeoutMS: 10000, socketTimeoutMS: 30000, waitQueueTimeoutMS: 20000, maxPoolSize: 20 };
export function clientFromUri(uri: string): MongoClient {
  const { local } = validateURI(uri);
  return new MongoClient(uri, { ...COMMON, ...(!local ? { tlsAllowInvalidCertificates: false } : {}) });
}
export interface TokenCredentialLike { getToken(scope: string, options: { abortSignal: AbortSignal }): Promise<{ token: string; expiresOnTimestamp: number } | null>; }
export async function tokenCallback(params: OIDCCallbackParams, credential: TokenCredentialLike, now = Date.now): Promise<OIDCResponse> {
  requireValue(params.version === 1, "Unsupported OIDC callback version");
  params.timeoutContext.throwIfAborted();
  const token = await credential.getToken(AZURE_SCOPE, { abortSignal: params.timeoutContext });
  params.timeoutContext.throwIfAborted();
  requireValue(token && typeof token.token === "string" && token.token.trim() && Number.isFinite(token.expiresOnTimestamp), "Missing or invalid OIDC token");
  const seconds = Math.floor((token.expiresOnTimestamp - now()) / 1000);
  requireValue(Number.isSafeInteger(seconds) && seconds > 0, "Expired OIDC token");
  return { accessToken: token.token, expiresInSeconds: seconds };
}
/** Validates endpoint/auth mode before optional credential construction. Never logs secrets. */
export async function clientFromEnvironment(env: NodeJS.ProcessEnv = process.env, credentialFactory?: () => Promise<TokenCredentialLike>): Promise<MongoClient> {
  const mode = env.MONGO_AUTH_MODE;
  if (mode === "connection-string" || mode === "local") {
    requireValue(!env.MONGO_OIDC_USERNAME, "OIDC username forbidden in native mode");
    const uri = mode === "local" ? env.DOCUMENTDB_URI : env.MONGO_CONNECTION_STRING;
    requireValue(uri, "Connection URI required");
    const parsed = validateURI(uri);
    requireValue(parsed.local === (mode === "local"), "Authentication mode/endpoint mismatch");
    return clientFromUri(uri);
  }
  requireValue(mode === "oidc", "Explicit MONGO_AUTH_MODE required");
  requireValue(!env.MONGO_CONNECTION_STRING && !env.DOCUMENTDB_URI, "OIDC forbids native connection strings");
  const host = env.MONGO_HOST ?? "";
  requireValue(AZURE_HOST.test(host), "Invalid OIDC endpoint");
  const username = env.MONGO_OIDC_USERNAME ?? "";
  requireValue(username.length <= 256 && !/[\s<>\u0000-\u001f\u007f]/.test(username), "Invalid OIDC username");
  const credential = credentialFactory ? await credentialFactory() : new (await import("@azure/identity")).DefaultAzureCredential({ processTimeoutInMs: 10000 });
  return new MongoClient(`mongodb+srv://${host}/`, { ...COMMON, tlsAllowInvalidCertificates: false,
    authMechanism: "MONGODB-OIDC", ...(username ? { auth: { username, password: "" } } : {}),
    authMechanismProperties: {
      ALLOWED_HOSTS: [host.endsWith(".global.mongocluster.cosmos.azure.com") ? "*.global.mongocluster.cosmos.azure.com" : "*.mongocluster.cosmos.azure.com"],
      OIDC_CALLBACK: (params: OIDCCallbackParams) => tokenCallback(params, credential),
    },
  });
}
