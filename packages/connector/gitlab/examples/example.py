"""GitLab connector demo — "ask my project".

Pull a GitLab project's issues and merge requests (with comments) into cognee
memory, incrementally, with forget-on-delete. Works with no LLM key: cognee
then extracts the graph with its local GLiNER model and embeds with fastembed.

Setup:

    pip install cognee-community-connector-gitlab
    export GITLAB_PROJECT="group/project"        # path or numeric id
    export GITLAB_TOKEN="glpat-…"               # read_api scope; needed for comments
    # optional: export GITLAB_URL="https://gitlab.example.com"   (self-hosted)

Run it:

    python examples/example.py
"""

import asyncio
import os

import cognee

from cognee_community_connector_gitlab import gitlab_source

DATASET_NAME = "gitlab_project"

# Routing kwargs shared by every remember() call. max_rows_per_table=0 keeps
# cognee reading the whole staging table, so forget-on-delete compares against
# the entire synced corpus.
REMEMBER_KWARGS = {"primary_key": "id", "write_disposition": "merge", "max_rows_per_table": 0}


async def main():
    if not os.environ.get("GITLAB_PROJECT"):
        print("Set GITLAB_PROJECT (and GITLAB_TOKEN for comments), then re-run.")
        return

    # ── First sync: full backfill ──────────────────────────────────────────
    print("=== GitLab sync #1 (backfill) ===")
    print(await cognee.remember(gitlab_source(), dataset_name=DATASET_NAME, **REMEMBER_KWARGS))

    # Without an LLM key recall() returns matching text chunks.
    results = await cognee.recall(
        query_text="Which issues mention a crash on startup?", datasets=[DATASET_NAME]
    )
    print(results)

    # ── Second sync: incremental delta + forget-on-delete ──────────────────
    # Re-running with the same dataset reuses the persisted cursor: only items
    # updated since sync #1 are fetched, and anything deleted in GitLab is
    # removed from memory by orphan_cleanup.
    print("=== GitLab sync #2 (incremental) ===")
    print(await cognee.remember(gitlab_source(), dataset_name=DATASET_NAME, **REMEMBER_KWARGS))


if __name__ == "__main__":
    asyncio.run(main())
