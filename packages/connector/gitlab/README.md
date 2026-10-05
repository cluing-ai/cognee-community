# cognee-community-connector-gitlab

A GitLab data-source connector for [cognee](https://github.com/topoteretes/cognee): sync a
project's **issues and merge requests, with their comments**, into memory — "ask my project".

It exposes a `dlt` source you hand to `cognee.remember(...)`, reusing cognee's existing DLT
ingestion path (`resolve_dlt_sources` → `ingest_dlt_source` → `orphan_cleanup`) in
*document mode*, so you get **incremental re-sync** (upsert by GitLab id, `merge` write
disposition, `updated_at` cursor) and **forget-on-delete** (items deleted in GitLab are
emitted as hard-deletes and purged from memory on the next sync) with no core changes.
Works with or without an LLM key.

## Install

```bash
uv pip install cognee-community-connector-gitlab
# or, from this monorepo:
cd packages/connector/gitlab && uv sync --all-extras
```

## Usage

```python
import cognee
from cognee_community_connector_gitlab import gitlab_source

await cognee.remember(
    gitlab_source(project="group/project"),  # token from GITLAB_TOKEN
    dataset_name="gitlab_project",
    primary_key="id",
    write_disposition="merge",  # incremental upsert by GitLab id
    max_rows_per_table=0,  # unlimited: orphan-cleanup sees the whole corpus
)

results = await cognee.recall(
    query_text="Which merge requests touched the login flow?",
    datasets=["gitlab_project"],
)
```

Re-running `remember(...)` with the same dataset syncs only items updated since the last
run and forgets items that were deleted. See `examples/example.py` for the full flow.

> **`write_disposition="merge"` is required** — the add pipeline defaults to `"replace"`,
> which would wipe the synced project on the second sync.

## Configuration

All from environment variables; nothing in code or in the repository.

| Variable | Required | Meaning |
|---|---|---|
| `GITLAB_PROJECT` | yes | Project path (`group/name`) or numeric id |
| `GITLAB_TOKEN` | for comments / private projects | Personal access token, `read_api` scope. Sent as `PRIVATE-TOKEN`; read-only |
| `GITLAB_URL` | no | Instance URL, default `https://gitlab.com`. Set it for self-hosted instances |

Arguments to `gitlab_source(...)` override the environment: `project`, `base_url`, `token`,
`kinds=("issues", "merge_requests")`, `include_comments=True`.

## How sync and forget-on-delete work

- **Primary key**: the GitLab global `id`, one dlt table per kind (`gitlab_issues`,
  `gitlab_merge_requests`), so ids never collide.
- **Cursor**: `updated_at`, kept in dlt's per-resource state together with the set of ids
  seen last run. Each run lists the project's current items (100 per request, no bodies
  beyond the listing) and fetches comments only for items newer than the cursor or never
  seen before.
- **Deletion**: GitLab has no deletion feed, so the listing doubles as an id sweep. Items
  that vanished are emitted with the `_deleted` hard-delete marker; dlt drops them on
  `merge` and cognee's `orphan_cleanup` removes them from the graph, vector and relational
  stores. **Closed or merged is not deleted** — those items stay in memory with their new
  state. An empty sweep while items were known is treated as a failed listing, not a wipe.
- **Comments**: non-system notes are folded into their parent's text, oldest first. GitLab
  bumps the parent's `updated_at` when a note is added, so comments ride the parent's cursor.
- **Rate limits and pagination**: `Link: rel="next"` is followed; 429/5xx are retried
  honouring `Retry-After` and `RateLimit-Reset`; any other HTTP error aborts the run so a
  partial listing never drives deletions.
- **Document mode**: the source declares `cognee_document_source = "gitlab"`, so each row is
  ingested as a text document (`# {title}\n\n{content}`) through normal cognify, with
  `url` and `id` kept in metadata.

## Testing

```bash
uv run pytest tests/
```

The tests fake the GitLab API (no network, no token, no model download) and include an
offline end-to-end run that drives the source through a real `dlt` merge to prove the
delete marker physically removes the row — exactly what cognee's `orphan_cleanup`
reconciles against.

## Not covered (yet)

Wiki pages. GitLab's wiki API returns no timestamps and no pagination, so a wiki resource
needs a different design (full fetch + content hash in state, `slug` as key).
