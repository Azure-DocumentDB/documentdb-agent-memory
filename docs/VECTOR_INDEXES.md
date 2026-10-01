# Vector indexes on Azure DocumentDB

Everything here is from running the same probe against both a local
`documentdb-local` container and a managed Azure DocumentDB M10 cluster. Where
docs and observed behavior disagreed, observed behavior won.

## Support matrix

| Kind | Local container (0.117.0) | Managed M10 | Minimum managed tier |
|---|---|---|---|
| `vector-ivf` | ✅ create + search | ✅ create + search | M10 |
| `vector-hnsw` | ✅ create + search | ❌ rejected | M30 |
| `vector-diskann` | ❌ not in build | ❌ rejected | M40 |

Both report `buildInfo.version` `7.0.0` and `maxWireVersion` 21.

## Local is a superset of a small managed cluster

This is the finding that matters, and it runs opposite to the usual assumption
that the local container is the limited one.

HNSW **works locally and fails in the cloud** on M10:

```
hnsw index is not supported for this cluster tier   (code 115, CommandNotSupported)
```

DiskANN fails in both, for different reasons — locally the kind does not exist
at all (`Invalid search index kind`, tried four spellings), on M10 it is tier-gated
(`The diskann index is not enabled for this cluster tier`).

So a green local test suite is **not** evidence that your index kind will deploy.
The only kind that works in both places is IVF, which is why it is the default
and why choosing anything else should be a deliberate, tier-aware decision.

These are tier gates, not build flags: no cluster-level setting turns HNSW on at
M10. `getParameter` is not supported on either deployment, so you cannot probe
capability that way — attempt the index and handle rejection.

## Pre-filtering requires a filter index, created first

Filtered vector search — the "search only this thread's memories" case — needs a
plain index on the filter field, and it must exist before the search runs:

```
The index for filter path 'thread_id' was not found,
please check whether the index is created.
```

This behaves **identically** on local and managed, including the error text.

Correct order:

```python
db.command({"createIndexes": "summaries",
            "indexes": [{"name": "thread_id_filter", "key": {"thread_id": 1}}]})
# ... then the vector index
```

A `cosmosSearchOptions` kind of `vector-filter` does **not** exist; the filter
index is an ordinary B-tree index. `ensure_filter_indexes()` wraps this, and
`setup()` calls it for the shared schema's filter fields.

Because per-session retrieval is *always* the pre-filter case, adapters create
these automatically rather than documenting the requirement and hoping.

## Notes

- `:latest` was **0.117.0**, trailing the v1.0-RC1 release. "Latest" is not latest;
  pin a digest if reproducibility matters.
- The local container serves TLS with a self-signed cert, so local clients need
  `tlsAllowInvalidCertificates=true`. The managed service must not use it.
- Similarity values used: `COS`, `L2`, `IP`. `COS` throughout here.

## Reproducing

```bash
export DOCUMENTDB_USERNAME=appuser DOCUMENTDB_PASSWORD='...'
docker compose -f docker/docker-compose.yml up -d
cd packages/langgraph-checkpoint-documentdb && pytest -q
```

`test_unsupported_kind_raises_actionable_error` and
`test_prefilter_without_filter_index_fails` encode the findings above. If either
starts behaving differently, the platform changed and this document is stale.
