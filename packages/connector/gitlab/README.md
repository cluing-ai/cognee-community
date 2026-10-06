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
| `GITLAB_MAX_CONTENT_CHARS` | no | Cap on one rendered document, default `32000`; `0` = no cap. Header and description come first, comments are kept oldest-first while they fit, and the text ends with how many were left out. Keeps one 200-comment thread from setting the memory limit of the whole sync |

Arguments to `gitlab_source(...)` override the environment: `project`, `base_url`, `token`,
`kinds=("issues", "merge_requests")`, `include_comments=True`, `max_content_chars=32000`.

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
- **Comments**: non-system notes are folded into their parent's text, oldest first. A new note
  moves the parent's `updated_at` `[unverified against a live instance; the API docs do not state
  it]`, so comments ride the parent's cursor; an edited or deleted note may not, and then reaches
  memory only when the parent is next touched. The Confluence connector has the same caveat.
- **Rate limits and pagination**: `Link: rel="next"` is followed; listings are ordered by
  `created_at` (append-only, so an edit mid-listing cannot hide an item from the sweep);
  429/5xx are retried honouring `Retry-After` (seconds or HTTP-date) and `RateLimit-Reset`, each
  wait capped at 60 s, five attempts; any other HTTP error aborts the run so a partial listing
  never drives deletions. gitlab.com caps offset pagination at 50,000 items per listing
  `[unverified]`; keyset pagination is not enabled because older self-hosted instances reject it.
- **Own pipeline state**: the source carries cognee's `cognee_pipeline_scope` marker
  (`gitlab:<url>:<project>`), so two projects in two datasets never share a cursor or id set.
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
