# Validation record — schema v2 / adapters 0.2.0

Tested October12,2026. Results below distinguish offline/framework contracts from actual managed-database behavior. No live language-model calls were made. Repository publication is separate from publishing packages to PyPI/npm.

## Managed Azure results

A disposable Azure DocumentDB M30 in West US2, one shard,128GiB storage, MongoDB8.0 compatibility and HA disabled was used. TLS certificate/hostname verification remained enabled and the firewall allowed only the test client's address. All data was synthetic and isolated from existing workloads.

| Gate | Native SCRAM | Microsoft Entra OIDC |
| --- | --- | --- |
| Python suite with real database-backed framework cases | 30 passed | 30 passed |
| TypeScript compound live gate | Passed | Passed |
| 100 simultaneous cold-thread appends | 100 acknowledged, seq0–99 | 100 acknowledged, seq0–99 |
| Filtered summary recall, namespace/thread separation | Passed | Passed |
| Both Python→TypeScript and TypeScript→Python setup orders | Passed | Passed |
| Bidirectional deletion across all five collections | Passed | Passed |
| Exact LangGraph checkpoint resumed in a different OS process | Passed | Passed |
| Legacy fixture refused without changing index catalog | Passed | Passed |

The Python suite includes some offline-only security/helper cases alongside the real database framework cases; “30 passed” is the suite count, not30independent cloud scenarios. The TypeScript live gate also verifies allocator initialization on a Python-shaped session, same-thread-ID histories in two namespaces, all-five lifecycle counts, and actual AI SDK7.0.138 tool-step/message handling with a deterministic published mock model plus real database storage. No model-service success is implied.

The cross-adapter coordinator passed **45 process stages per authentication mode**. It created genuine LangGraph checkpoint/pending-write payloads in Python, persisted/read messages and summaries through TypeScript, checked the common catalog in both setup orders, and deleted via each adapter while preserving the other namespace and neighboring thread. A deterministic graph paused at count11; a separate process loaded the exact checkpoint ID and resumed to16 without rerunning the preparation node. Both adapters refused a deliberately malformed legacy checkpoint while leaving the catalog unchanged. Cleanup removed only generated test namespaces, never whole databases.

Implementation tested: commit `29dd4ea3f71916a53799f109d6c338ac18886bb4`. Later root documentation/harness-default changes do not change the adapters. The coordinator was invoked with an explicit Python executable during managed testing; its default now uses the active interpreter for normal standalone checkout usability.

## Local and package checks

- Python3.12.13, LangGraph1.2.15, langgraph-checkpoint4.3.0, PyMongo4.18.3 and Azure Identity1.26.0. **29offline tests pass** with the dedicated managed-only case deselected, not counted as successful cloud coverage. Includes genuine sync/async StateGraph compile/invoke, interruption/replay, nested graphs, parallel failure recovery, pending-write idempotence/order, typed serialization and security/schema contracts.
- The Python Entra factory's unsupported PyMongo `ALLOWED_HOSTS` machine-callback option was identified in review, removed and regression-tested with the real installed option parser before live runs. Exact Azure seed validation, normal SRV/TLS enforcement and fixed token scope remain.
- Python wheel/sdist built; the wheel installed into an independent consumer environment and imports from site-packages. Dependency compatibility passed. The hash-locked84dependency audit reported zero known vulnerabilities with no ignored advisories.
- Node24.21.0, MongoDB7.7.0, AI SDK7.0.138, optional Azure Identity4.13.3. **24compiled tests pass**, zero skipped; build/typecheck and full/production dependency audits pass with zero findings. The real SDK mock model exercises two-step tool use; all step response messages are persisted, and system messages are supplied as instructions.
- Version0.2.0 npm tarball built, inspected and installed outside source; public import smoke and audit passed. No npm/PyPI upload occurred.
- **13offline cross-process coordinator tests** pass for consent, exact generated scope, environment translation, sanitized subprocess failures and syntax. Missing live consent fails before network and is never counted as an integration pass.
- Full-history secret scanning reported no credential matches. This is a scanner outcome, not an absolute guarantee. No live credentials or infrastructure identifiers are included in source, tests or this record.

## Limits and migration

The application must authenticate and authorize the namespace/thread. Storage filters are not access control against a caller with arbitrary database credentials.

Quiesce writers before deleting a thread. Five-collection deletion is retryable while quiescent but is not a distributed transaction. Atomic range reservation may leave sequence gaps after failed or partially committed inserts; retrying an unknown write outcome is not an exactly-once guarantee. The tested100-call concurrency scenario is not a throughput or unlimited-contention promise.

Synthetic3D COS/IVF vectors with numLists1 verify filtered recall—not semantic relevance, production ANN quality or every tier/index combination. A new client/process reading persisted state is not a database server-crash, regional recovery or HA test. The optional pinned local release-candidate Compose recipe was not executed for this release and is not claimed equivalent to the managed tests.

Schema/API v2 is breaking from prototype0.1: use a fresh database or perform a separately reviewed offline migration. Setup never drops conflicting old indexes or migrates legacy records. Python serialization forbids unapproved constructors and pickle fallback; hostile stored envelopes may become inert values rather than universally raising an exception. Optional framework maintenance methods not implemented by the package are explicitly documented in its README.

## CI and cleanup

Pinned GitHub-owned CI runs offline framework/package/security checks without cloud credentials. Real managed and cross-adapter acceptance is recorded here, not silently delegated to skipped CI tests. Release publication requires the exact candidate's CI, reviewed documentation, package checks, cloud teardown and anonymous checkout proof.

All owned managed test resources are deleted by the release operator after collecting results. Deletion verification and exact repository/CI revisions are retained with the release evidence; no existing workload is targeted. Users running their own tests must explicitly remove their own resource groups to stop ongoing charges.
