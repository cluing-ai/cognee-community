"""Unit tests for the GitLab connector.

The GitLab REST API v4 is fully faked via ``FakeGitLabSession`` — no network,
no live token, no model download — so these run in CI. Coverage:

  - Link-header pagination is followed; the next link's params are reused
  - 429 / 5xx are retried honouring Retry-After and RateLimit-Reset
  - a non-retryable HTTP error aborts the sync (no partial inventory)
  - first sync yields every item and records the cursor + id set
  - incremental re-sync yields ONLY items updated since the cursor
  - a no-change run is a no-op
  - an item new to the corpus but older than the cursor is still ingested
  - closed / merged items are NOT treated as deleted
  - items that vanish from the sweep become hard-delete markers
  - an empty sweep does not mass-delete and preserves state
  - non-system comments are folded into the item text; system notes are not
  - merge requests use their own table and carry branch info
  - the source declares the cognee document marker and merge + hard_delete
  - configuration comes from environment variables
  - a real dlt merge removes the marked row (end-to-end forget-on-delete)

The end-to-end "deletion removes from memory" guarantee is cognee's existing
``orphan_cleanup`` path; here we prove the connector emits the markers that
drive it, and that dlt acts on them.
"""

from __future__ import annotations

import json
from urllib.parse import parse_qs, urlparse

import pytest

from cognee_community_connector_gitlab.gitlab import (
    GITLAB_SOURCE_NAME,
    _api_get,
    _paginate,
    _render_content,
    gitlab_source,
    sync_items,
)

BASE_URL = "https://gitlab.example.com"
PROJECT = "group/demo"
PROJECT_URL = f"{BASE_URL}/api/v4/projects/group%2Fdemo"


# ---------------------------------------------------------------------------
# Fake GitLab REST API
# ---------------------------------------------------------------------------
def _item(item_id, *, iid=None, updated, title="", state="opened", description="", **extra):
    row = {
        "id": item_id,
        "iid": iid or item_id,
        "title": title or f"Item {item_id}",
        "state": state,
        "description": description,
        "labels": extra.pop("labels", []),
        "author": {"username": extra.pop("author", "alice")},
        "created_at": "2026-01-01T00:00:00.000Z",
        "updated_at": updated,
        "web_url": f"{BASE_URL}/{PROJECT}/-/issues/{iid or item_id}",
    }
    row.update(extra)
    return row


def _note(body, *, system=False, author="bob"):
    return {"body": body, "system": system, "author": {"username": author}}


class _Resp:
    def __init__(self, payload, status=200, headers=None):
        self._payload = payload
        self.status_code = status
        self.headers = headers or {}

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def json(self):
        return self._payload


class FakeGitLabSession:
    """Minimal stand-in for a ``requests`` session hitting GitLab v4.

    ``items``: {kind: [item, ...]}; ``notes``: {(kind, iid): [note, ...]}.
    ``page_size`` splits listings into Link-paginated pages. ``fail`` is a list
    of ``(status, headers)`` responses to return first, for retry tests.
    """

    def __init__(self, items, notes=None, *, page_size=100, fail=None):
        self.items = items
        self.notes = notes or {}
        self.page_size = page_size
        self.fail = list(fail or [])
        self.calls: list[tuple[str, dict]] = []

    def get(self, url, params=None):
        params = dict(params or {})
        self.calls.append((url, params))
        if self.fail:
            status, headers = self.fail.pop(0)
            return _Resp({"message": "nope"}, status, headers)

        parsed = urlparse(url)
        query = {k: v[0] for k, v in parse_qs(parsed.query).items()}
        query.update(params)
        parts = parsed.path.split("/")
        # .../projects/<proj>/<kind>              → listing
        # .../projects/<proj>/<kind>/<iid>/notes  → notes
        if parts[-1] == "notes":
            kind = parts[-3]
            iid = int(parts[-2])
            return _Resp(self.notes.get((kind, iid), []))
        kind = parts[-1]
        rows = self.items.get(kind, [])
        page = int(query.get("page", 1))
        size = self.page_size  # the server caps per_page; the client cannot raise it
        chunk = rows[(page - 1) * size : page * size]
        headers = {}
        if page * size < len(rows):
            headers["Link"] = f'<{PROJECT_URL}/{kind}?page={page + 1}&per_page={size}>; rel="next"'
        return _Resp(chunk, headers=headers)


def _run(session, state=None, kind="issues", **kw):
    state = state if state is not None else {}
    rows = list(sync_items(session, BASE_URL, PROJECT, kind, state, **kw))
    return rows, state


# ---------------------------------------------------------------------------
# HTTP layer
# ---------------------------------------------------------------------------
def test_paginate_follows_link_header_and_drops_initial_params():
    items = [_item(i, updated="2026-01-01T00:00:00.000Z") for i in range(1, 6)]
    session = FakeGitLabSession({"issues": items}, page_size=2)

    got = list(_paginate(session, f"{PROJECT_URL}/issues", {"state": "all"}))

    assert [r["id"] for r in got] == [1, 2, 3, 4, 5]
    assert len(session.calls) == 3
    # The first call carries the filters; later calls use the server's next URL.
    assert session.calls[0][1]["state"] == "all"
    assert "page=2" in session.calls[1][0] and session.calls[1][1] == {}


def test_api_get_retries_rate_limit_honouring_retry_after_and_reset(monkeypatch):
    import time as _time

    slept = []
    now = _time.time()
    session = FakeGitLabSession(
        {"issues": []},
        fail=[(429, {"Retry-After": "3"}), (503, {"RateLimit-Reset": str(int(now) + 2)})],
    )

    response = _api_get(session, f"{PROJECT_URL}/issues", {}, sleep=slept.append)

    assert response.status_code == 200
    assert slept[0] == 3.0
    assert 0.0 <= slept[1] <= 2.5
    assert len(session.calls) == 3


def test_non_retryable_http_error_aborts_the_sync():
    session = FakeGitLabSession({"issues": []}, fail=[(401, {})])

    with pytest.raises(RuntimeError, match="HTTP 401"):
        _run(session)


# ---------------------------------------------------------------------------
# First sync, incremental sync, deletion
# ---------------------------------------------------------------------------
def test_first_sync_yields_all_items_and_records_cursor_and_ids():
    session = FakeGitLabSession(
        {
            "issues": [
                _item(1, updated="2026-01-01T10:00:00.000Z", description="first"),
                _item(2, updated="2026-01-02T10:00:00.000Z", description="second"),
            ]
        }
    )

    rows, state = _run(session)

    assert [r["id"] for r in rows] == ["1", "2"]
    assert all(r["_deleted"] is False for r in rows)
    assert rows[0]["title"] == "Item 1" and "first" in rows[0]["content"]
    assert rows[0]["kind"] == "issues" and rows[0]["project"] == PROJECT
    assert state["last_updated"] == "2026-01-02T10:00:00.000Z"
    assert state["known_ids"] == ["1", "2"]


def test_incremental_yields_only_items_updated_since_cursor():
    session = FakeGitLabSession(
        {
            "issues": [
                _item(1, updated="2026-01-01T10:00:00.000Z"),
                _item(2, updated="2026-01-03T10:00:00.000Z", description="edited"),
            ]
        }
    )
    state = {"last_updated": "2026-01-02T10:00:00.000Z", "known_ids": ["1", "2"]}

    rows, state = _run(session, state)

    assert [r["id"] for r in rows] == ["2"]
    assert "edited" in rows[0]["content"]
    assert state["last_updated"] == "2026-01-03T10:00:00.000Z"
    # Only the changed item had its comments fetched: 1 listing + 1 notes call.
    assert len(session.calls) == 2


def test_incremental_no_changes_is_a_noop():
    session = FakeGitLabSession({"issues": [_item(1, updated="2026-01-01T10:00:00.000Z")]})
    state = {"last_updated": "2026-01-01T10:00:00.000Z", "known_ids": ["1"]}

    rows, state = _run(session, state)

    assert rows == []
    assert state == {"last_updated": "2026-01-01T10:00:00.000Z", "known_ids": ["1"]}


def test_item_new_to_corpus_but_older_than_cursor_is_still_ingested():
    session = FakeGitLabSession(
        {
            "issues": [
                _item(1, updated="2026-01-05T00:00:00.000Z"),
                _item(9, updated="2025-06-01T00:00:00.000Z"),  # old, never seen
            ]
        }
    )
    state = {"last_updated": "2026-01-05T00:00:00.000Z", "known_ids": ["1"]}

    rows, state = _run(session, state)

    assert [r["id"] for r in rows] == ["9"]
    assert state["known_ids"] == ["1", "9"]
    assert state["last_updated"] == "2026-01-05T00:00:00.000Z"  # cursor never moves back


def test_closed_and_merged_items_are_updated_not_deleted():
    session = FakeGitLabSession(
        {
            "issues": [_item(1, updated="2026-01-02T00:00:00.000Z", state="closed")],
            "merge_requests": [_item(5, updated="2026-01-02T00:00:00.000Z", state="merged")],
        }
    )
    state = {"last_updated": "2026-01-01T00:00:00.000Z", "known_ids": ["1"]}
    rows, state = _run(session, state)
    assert [(r["id"], r["state"], r["_deleted"]) for r in rows] == [("1", "closed", False)]
    assert state["known_ids"] == ["1"]

    rows, _ = _run(
        session,
        {"last_updated": "2026-01-01T00:00:00.000Z", "known_ids": ["5"]},
        kind="merge_requests",
    )
    assert [(r["id"], r["state"], r["_deleted"]) for r in rows] == [("5", "merged", False)]


def test_vanished_item_emits_hard_delete_marker():
    session = FakeGitLabSession({"issues": [_item(1, updated="2026-01-01T00:00:00.000Z")]})
    state = {"last_updated": "2026-01-01T00:00:00.000Z", "known_ids": ["1", "2"]}

    rows, state = _run(session, state)

    assert rows == [{"id": "2", "_deleted": True}]
    assert state["known_ids"] == ["1"]


def test_empty_sweep_does_not_mass_delete_and_preserves_state():
    session = FakeGitLabSession({"issues": []})
    state = {"last_updated": "2026-01-01T00:00:00.000Z", "known_ids": ["1", "2"]}

    rows, state = _run(session, state)

    assert rows == []
    assert state["known_ids"] == ["1", "2"]
    assert state["last_updated"] == "2026-01-01T00:00:00.000Z"


# ---------------------------------------------------------------------------
# Content shape
# ---------------------------------------------------------------------------
def test_comments_are_folded_and_system_notes_skipped():
    session = FakeGitLabSession(
        {"issues": [_item(1, iid=7, updated="2026-01-01T00:00:00.000Z", description="body")]},
        notes={
            ("issues", 7): [
                _note("changed the description", system=True),
                _note("looks like a regression", author="carol"),
                _note("   "),
            ]
        },
    )

    rows, _ = _run(session)

    content = rows[0]["content"]
    assert "body" in content
    assert "Comments:" in content and "carol: looks like a regression" in content
    assert "changed the description" not in content
    assert session.calls[1][0].endswith("/issues/7/notes")


def test_include_comments_false_skips_notes_calls():
    session = FakeGitLabSession({"issues": [_item(1, updated="2026-01-01T00:00:00.000Z")]})

    rows, _ = _run(session, include_comments=False)

    assert len(rows) == 1 and len(session.calls) == 1


def test_merge_request_rows_carry_branches_and_use_mr_endpoint():
    session = FakeGitLabSession(
        {
            "merge_requests": [
                _item(
                    3,
                    iid=12,
                    updated="2026-01-01T00:00:00.000Z",
                    source_branch="fix/x",
                    target_branch="main",
                    labels=["bug"],
                )
            ]
        }
    )

    rows, _ = _run(session, kind="merge_requests")

    assert rows[0]["kind"] == "merge_requests" and rows[0]["labels"] == "bug"
    assert "Merge request !12" in rows[0]["content"]
    assert "Branches: fix/x → main" in rows[0]["content"]
    assert session.calls[0][0].endswith("/merge_requests")


def test_render_content_is_stable_across_updated_at_changes():
    a = _item(1, updated="2026-01-01T00:00:00.000Z", description="same")
    b = _item(1, updated="2026-02-01T00:00:00.000Z", description="same")
    assert _render_content(a, "Issue", []) == _render_content(b, "Issue", [])


def test_unknown_kind_is_rejected():
    with pytest.raises(ValueError, match="unknown kind"):
        _run(FakeGitLabSession({}), kind="wikis")


# ---------------------------------------------------------------------------
# dlt source wiring + configuration
# ---------------------------------------------------------------------------
def test_gitlab_source_declares_document_marker_merge_and_hard_delete():
    from cognee.tasks.ingestion.dlt_utils import document_source_tag

    source = gitlab_source(project=PROJECT, session=object())

    assert document_source_tag(source) == GITLAB_SOURCE_NAME == "gitlab"
    resources = source.resources
    assert set(resources) == {"gitlab_issues", "gitlab_merge_requests"}
    for name in resources:
        table = resources[name].compute_table_schema()
        assert table["write_disposition"] == "merge"
        assert table["columns"]["id"]["primary_key"] is True
        assert table["columns"]["_deleted"]["hard_delete"] is True
        assert table["columns"]["_deleted"]["data_type"] == "bool"


def test_gitlab_source_reads_configuration_from_environment(monkeypatch):
    monkeypatch.setenv("GITLAB_PROJECT", "42")
    monkeypatch.setenv("GITLAB_URL", "https://git.internal/")
    monkeypatch.setenv("GITLAB_TOKEN", "glpat-test")

    source = gitlab_source(kinds=["issues"], session=object())

    assert list(source.resources) == ["gitlab_issues"]
    # The token is never part of the source object (it only lives in the session).
    assert "glpat-test" not in json.dumps(source.discover_schema().to_dict(), default=str)


def test_gitlab_source_requires_project_and_known_kinds(monkeypatch):
    monkeypatch.delenv("GITLAB_PROJECT", raising=False)
    with pytest.raises(ValueError, match="GITLAB_PROJECT"):
        gitlab_source(session=object())
    with pytest.raises(ValueError, match="unknown kinds"):
        gitlab_source(project=PROJECT, kinds=["wikis"], session=object())


# ---------------------------------------------------------------------------
# End-to-end: a real dlt merge acts on the hard-delete marker
# ---------------------------------------------------------------------------
def _pipeline(dlt, tmp_path):
    db_path = (tmp_path / "gitlab.db").as_posix()
    return dlt.pipeline(
        pipeline_name="gitlab_test",
        destination=dlt.destinations.sqlalchemy(f"sqlite:///{db_path}"),
        dataset_name="gitlab_ds",
        pipelines_dir=str(tmp_path / "state"),
    )


def _ids(pipeline, table):
    with (
        pipeline.sql_client() as client,
        client.execute_query(f"SELECT id, state, content FROM {table} ORDER BY id") as cursor,
    ):
        return {row[0]: {"state": row[1], "content": row[2]} for row in cursor.fetchall()}


def test_forget_on_delete_end_to_end_through_a_real_dlt_merge(tmp_path):
    dlt = pytest.importorskip("dlt")
    pipeline = _pipeline(dlt, tmp_path)

    # Sync #1: two issues and one MR land in the destination.
    session1 = FakeGitLabSession(
        {
            "issues": [
                _item(1, updated="2026-01-01T10:00:00.000Z", description="a"),
                _item(2, updated="2026-01-02T10:00:00.000Z", description="b"),
            ],
            "merge_requests": [_item(7, updated="2026-01-02T10:00:00.000Z")],
        }
    )
    pipeline.run(gitlab_source(project=PROJECT, session=session1))
    assert set(_ids(pipeline, "gitlab_issues")) == {"1", "2"}
    assert set(_ids(pipeline, "gitlab_merge_requests")) == {"7"}

    # Sync #2: issue 2 deleted upstream, issue 1 edited and closed, MR unchanged.
    # The cursor + known ids were persisted in dlt state by run #1.
    session2 = FakeGitLabSession(
        {
            "issues": [
                _item(1, updated="2026-01-03T10:00:00.000Z", state="closed", description="a-edited")
            ],
            "merge_requests": [_item(7, updated="2026-01-02T10:00:00.000Z")],
        }
    )
    pipeline.run(gitlab_source(project=PROJECT, session=session2))

    issues = _ids(pipeline, "gitlab_issues")
    assert set(issues) == {"1"}  # "2" forgotten, "1" retained
    assert issues["1"]["state"] == "closed" and "a-edited" in issues["1"]["content"]
    assert set(_ids(pipeline, "gitlab_merge_requests")) == {"7"}
    # Only the changed issue was re-read (1 listing per kind + 1 notes call).
    assert len(session2.calls) == 3
