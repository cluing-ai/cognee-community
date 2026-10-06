"""GitLab connector for cognee — a ``dlt`` source that turns a project's issues
and merge requests (with their comments) into memory.

The source is handed straight to :func:`cognee.remember`::

    import cognee
    from cognee_community_connector_gitlab import gitlab_source

    await cognee.remember(
        gitlab_source(project="group/project"),   # token from GITLAB_TOKEN
        dataset_name="my_gitlab",
        primary_key="id",
        write_disposition="merge",   # incremental upsert by GitLab id
    )

Design
------
* **Auth** — a GitLab personal access token (``read_api`` scope is enough),
  sent as ``PRIVATE-TOKEN``. Read-only: the connector only issues ``GET``.
  The base URL is configurable from day one (``GITLAB_URL``), because
  self-hosted instances differ from gitlab.com in version and rate limits.
* **Document mode** — the source declares ``cognee_document_source = "gitlab"``
  (Notion does the same), so cognee routes every row through normal cognify as
  a text document built from the ``title`` and ``content`` columns, instead of
  the relational dlt-schema path.
* **Primary key** — the GitLab global ``id`` (one table per kind, so issue and
  merge-request ids never collide). With ``write_disposition="merge"`` a second
  run upserts instead of duplicating.
* **Incremental cursor** — ``updated_at``. Each run lists the project's current
  items (cheap: one page of 100 per request, no bodies fetched) and only items
  newer than the highest ``updated_at`` seen so far, or not seen before, have
  their comments fetched and are emitted. The cursor and the id set live in
  dlt's per-resource state, so re-running ``remember`` resumes where it left off.
  GitLab timestamps are ISO-8601 UTC strings of fixed shape, so they compare
  as strings. Known window: an item written in the same second as the cursor,
  after its page was read, is skipped on the next run until it changes again
  (second-granularity timestamps; the alternative, re-fetching ties every run,
  would make a no-change run emit rows, which the deployment proof relies on).
* **Listing order** — ``order_by=created_at``, ascending. Offset pagination over
  a key that changes while listing (``updated_at``) lets an edited item move
  between pages; ``created_at`` is append-only, so every live item is seen
  exactly once per sweep and deletion detection cannot misfire. gitlab.com caps
  offset pagination at 50,000 items per listing ``[unverified: GitLab docs,
  REST API pagination]``; projects beyond that need keyset pagination, which
  is not enabled here because older self-hosted instances reject it.
* **Own pipeline state** — the source carries cognee's ``cognee_pipeline_scope``
  marker (``gitlab:<base_url>:<project>``), so two projects or two instances in
  two datasets never share a cursor or a known-id set; without it every unscoped
  dlt source shares one pipeline name and alternating runs would refetch
  everything and emit spurious deletes.
* **Forget-on-delete** — GitLab has no deletion feed, so the listing above
  doubles as an id sweep against the previous run's ids. Items that vanished are
  emitted with the ``_deleted`` hard-delete marker; dlt removes those rows on
  ``merge`` and cognee's ``orphan_cleanup`` purges them from the graph, vector
  and relational stores. **Closed is not deleted**: closed and merged items are
  listed with ``state=all`` and stay in memory with their new state.
* **Comments** — non-system notes are folded into their parent's text, like the
  Confluence connector folds footer comments. One document per discussion. A
  new note moves the parent's ``updated_at`` ``[unverified against a live
  instance; the API docs do not state it]``; an edited or deleted note may not,
  so such a change reaches memory only when the parent is next touched.
* **Rate limits** — 429 and 5xx responses are retried, honouring ``Retry-After``
  (seconds or HTTP-date) and ``RateLimit-Reset``, each wait capped at 60 s,
  five attempts in total; any other HTTP error, or the fifth retryable one,
  aborts the run. An aborted run leaves staging and memory untouched, which is
  the safe failure: a partial listing must never drive deletions.
"""

from __future__ import annotations

import email.utils
import os
import re
import time
from collections.abc import Iterator
from datetime import UTC
from typing import Any, NamedTuple

import dlt
import requests
from cognee.shared.logging_utils import get_logger
from cognee.tasks.ingestion.dlt_utils import DOCUMENT_SOURCE_ATTR, PIPELINE_SCOPE_ATTR

logger = get_logger("gitlab_connector")

GITLAB_SOURCE_NAME = "gitlab"
DEFAULT_BASE_URL = "https://gitlab.com"


class Kind(NamedTuple):
    """One supported item kind: API path segment, dlt table name, human label."""

    path: str
    table: str
    label: str


KINDS: dict[str, Kind] = {
    "issues": Kind("issues", "gitlab_issues", "Issue"),
    "merge_requests": Kind("merge_requests", "gitlab_merge_requests", "Merge request"),
}

_PER_PAGE = 100
_MAX_RETRIES = 5
# Longest single wait between retries. A server-sent ``Retry-After: 3600`` must not park
# the sync for an hour inside a generator; the run aborts after _MAX_RETRIES instead.
_MAX_RETRY_DELAY = 60.0
# Cap on one rendered document (header + description + comments). One issue with
# a 200-comment thread must not dictate the memory limit of the whole sync:
# GLiNER memory grows with the chunks of a single document. 0 = no cap.
DEFAULT_MAX_CONTENT_CHARS = 32_000
_RETRY_STATUSES = {429, 500, 502, 503, 504}
_LINK_NEXT_RE = re.compile(r'<([^>]+)>;\s*rel="next"')


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------
def _make_session(token: str | None) -> Any:
    """Build a ``requests`` session; the token is optional for public reads."""
    session = requests.Session()
    session.headers.update({"Accept": "application/json"})
    if token:
        session.headers["PRIVATE-TOKEN"] = token
    return session


def _retry_delay(headers: Any, attempt: int, *, now: float | None = None) -> float:
    """Seconds to wait, capped at ``_MAX_RETRY_DELAY``.

    ``Retry-After`` as seconds or as an HTTP-date (RFC 7231 allows both), else
    the time until ``RateLimit-Reset`` (epoch seconds), else ``2**attempt``.
    """
    headers = headers or {}
    now = time.time() if now is None else now
    retry_after = headers.get("Retry-After") or headers.get("retry-after")
    if retry_after is not None:
        try:
            return min(_MAX_RETRY_DELAY, max(0.0, float(retry_after)))
        except (TypeError, ValueError):
            try:
                when = email.utils.parsedate_to_datetime(str(retry_after))
                if when.tzinfo is None:
                    when = when.replace(tzinfo=UTC)
                return min(_MAX_RETRY_DELAY, max(0.0, when.timestamp() - now))
            except (TypeError, ValueError):
                pass
    reset = headers.get("RateLimit-Reset") or headers.get("ratelimit-reset")
    try:
        return min(_MAX_RETRY_DELAY, max(0.0, float(reset) - now))
    except (TypeError, ValueError):
        pass
    return min(_MAX_RETRY_DELAY, float(2**attempt))


def _api_get(session: Any, url: str, params: dict | None = None, *, sleep=time.sleep) -> Any:
    """GET a GitLab API URL, retrying rate-limit and server errors.

    Returns the ``requests`` response so callers can read pagination headers.
    Any non-retryable HTTP error raises; a partial listing must abort the run.
    """
    for attempt in range(_MAX_RETRIES):
        response = session.get(url, params=params or {})
        status = response.status_code
        if status in _RETRY_STATUSES and attempt < _MAX_RETRIES - 1:
            delay = _retry_delay(response.headers, attempt)
            logger.warning(
                "GitLab: HTTP %s on %s — retrying in %.1fs (%d/%d).",
                status,
                url,
                delay,
                attempt + 1,
                _MAX_RETRIES,
            )
            sleep(delay)
            continue
        response.raise_for_status()
        return response
    raise RuntimeError("unreachable")  # pragma: no cover


def _next_link(response: Any) -> str | None:
    """Return the ``rel="next"`` URL from the ``Link`` header, if any."""
    link = (response.headers or {}).get("Link") or ""
    match = _LINK_NEXT_RE.search(link)
    return match.group(1) if match else None


def _paginate(session: Any, url: str, params: dict) -> Iterator[dict]:
    """Yield items across all pages, following the ``Link: rel="next"`` header.

    GitLab's next link already carries ``page``/``per_page`` and every original
    filter, so subsequent requests drop the initial params.
    """
    next_url: str | None = url
    next_params: dict | None = {**params, "per_page": _PER_PAGE}
    while next_url:
        response = _api_get(session, next_url, next_params)
        yield from response.json() or []
        next_url = _next_link(response)
        next_params = None


# ---------------------------------------------------------------------------
# Rows
# ---------------------------------------------------------------------------
def _project_path(base_url: str, project: str) -> str:
    """``/api/v4/projects/<url-encoded project>`` for a path or numeric id."""
    from urllib.parse import quote

    return f"{base_url}/api/v4/projects/{quote(str(project), safe='')}"


def _render_content(
    item: dict, label: str, comments: list[str], max_chars: int = DEFAULT_MAX_CONTENT_CHARS
) -> str:
    """Markdown document for one issue / merge request plus its comments.

    ``updated_at`` is deliberately left out of the text: cognee derives the
    document identity from the whole row's content hash, so what matters is
    that unchanged text stays byte-identical and is never re-chunked or
    re-cognified for a timestamp-only change.

    ``max_chars`` bounds the whole text (0 = unbounded) exactly: the rendered
    string is never longer than ``max_chars``, and a document that fits is
    never cut. The header and the description have priority; the largest
    prefix of comments (oldest first) that fits together with the closing
    "omitted" line is kept. Truncation is a pure function of the inputs, so a
    capped item still renders identically from run to run.
    """
    author = (item.get("author") or {}).get("username") or ""
    labels = ", ".join(item.get("labels") or [])
    header = [
        f"{label} !{item.get('iid')}"
        if label == "Merge request"
        else f"{label} #{item.get('iid')}",
        f"State: {item.get('state') or ''}",
        f"Author: {author}",
    ]
    if labels:
        header.append(f"Labels: {labels}")
    if item.get("source_branch"):
        header.append(f"Branches: {item.get('source_branch')} → {item.get('target_branch')}")
    description = (item.get("description") or "").strip()
    body = "\n\n".join(p for p in ("\n".join(header), description) if p)

    if not max_chars:
        return body + ("\n\nComments:\n\n" + "\n\n".join(comments) if comments else "")

    def _omitted_line(count: int) -> str:
        return f"\n\n[{count} more comments omitted]" if count else ""

    if len(body) > max_chars:
        tail = f"\n\n[{len(comments)} comments omitted]" if comments else ""
        marker = "\n\n[description truncated: {omitted} characters omitted]"
        # Prefer body + marker + tail; if the cap is too small for the tail, drop it; if it is
        # too small even for the marker, the hard cut wins. Never longer than max_chars.
        for suffix in (tail, ""):
            omitted = len(body) - max_chars
            for _ in range(2):  # the marker's own length depends on the digit count
                text = marker.format(omitted=omitted)
                keep = max(0, max_chars - len(text) - len(suffix))
                omitted = len(body) - keep
            candidate = body[:keep] + marker.format(omitted=omitted) + suffix
            if len(candidate) <= max_chars and keep > 0:
                return candidate
        return body[:max_chars]

    if not comments:
        return body

    # Largest k such that body + "Comments:" + the first k comments + the
    # omitted line for the rest fits in max_chars. Cumulative lengths make
    # this a single backwards scan.
    head = body + "\n\nComments:"
    cumulative = [0]
    for comment in comments:
        cumulative.append(cumulative[-1] + len("\n\n") + len(comment))
    for k in range(len(comments), -1, -1):
        total = len(head) + cumulative[k] + len(_omitted_line(len(comments) - k))
        if total <= max_chars:
            break
    kept = comments[:k]
    rendered = head + "".join("\n\n" + c for c in kept) + _omitted_line(len(comments) - k)
    if k == 0 and len(rendered) > max_chars:
        # Not even the omitted line fits after the body: the body wins, nothing else.
        return body
    return rendered


def _item_to_row(
    item: dict,
    kind: str,
    project: str,
    comments: list[str],
    max_content_chars: int = DEFAULT_MAX_CONTENT_CHARS,
) -> dict[str, Any]:
    label = KINDS[kind].label
    return {
        "id": str(item.get("id")),
        "iid": int(item.get("iid") or 0),
        "kind": kind,
        "project": str(project),
        "title": item.get("title") or "",
        "state": item.get("state") or "",
        "labels": ", ".join(item.get("labels") or []),
        "author": (item.get("author") or {}).get("username") or "",
        "created_at": item.get("created_at") or "",
        "updated_at": item.get("updated_at") or "",
        "url": item.get("web_url") or "",
        "content": _render_content(item, label, comments, max_content_chars),
        # Hard-delete marker (always False for live items). Vanished items are
        # emitted separately with _deleted=True.
        "_deleted": False,
    }


def _deleted_row(item_id: str) -> dict[str, Any]:
    """Minimal row that instructs dlt to hard-delete an item by id."""
    return {"id": str(item_id), "_deleted": True}


def _item_comments(session: Any, project_url: str, kind: str, iid: int) -> list[str]:
    """Non-system notes of one item, oldest first, as ``author: text`` lines."""
    path = KINDS[kind].path
    texts: list[str] = []
    for note in _paginate(
        session, f"{project_url}/{path}/{iid}/notes", {"sort": "asc", "order_by": "created_at"}
    ):
        if note.get("system"):
            continue  # "changed the description", "closed" … are not content
        body = (note.get("body") or "").strip()
        if body:
            author = (note.get("author") or {}).get("username") or ""
            texts.append(f"{author}: {body}" if author else body)
    return texts


# ---------------------------------------------------------------------------
# Sync (pure given a session + state dict — unit-testable)
# ---------------------------------------------------------------------------
def sync_items(
    session: Any,
    base_url: str,
    project: str,
    kind: str,
    state: dict,
    *,
    include_comments: bool = True,
    max_content_chars: int = DEFAULT_MAX_CONTENT_CHARS,
) -> Iterator[dict[str, Any]]:
    """Yield changed items of one kind since the last run, plus delete markers.

    One listing pass enumerates the project's *current* items (no bodies
    beyond what the list returns): that set drives deletion detection, while
    items newer than the stored cursor, or never seen, have their comments
    fetched and are emitted. ``last_updated`` and ``known_ids`` are advanced in
    ``state`` so the next run is a no-op when nothing changed.
    """
    if kind not in KINDS:
        raise ValueError(f"unknown kind {kind!r}; expected one of {sorted(KINDS)}")
    path, _table, label = KINDS[kind]
    project_url = _project_path(base_url, project)

    known_ids: set[str] = set(state.get("known_ids", []))
    last_updated: str = state.get("last_updated", "")
    newest = last_updated
    current_ids: set[str] = set()
    changed = 0

    # created_at is append-only: an edit while listing cannot move an item across a
    # page boundary, so the sweep sees every live item exactly once (see module docs).
    for item in _paginate(
        session,
        f"{project_url}/{path}",
        {"state": "all", "order_by": "created_at", "sort": "asc"},
    ):
        item_id = str(item["id"])
        current_ids.add(item_id)
        updated = item.get("updated_at") or ""
        # Skip only items already ingested and unchanged. An id absent from
        # known_ids is fetched regardless of timestamp, so an old item that is
        # new to us (moved in, un-confidentialed, tied at the cursor) is kept.
        if item_id in known_ids and updated <= last_updated:
            continue
        if updated > newest:
            newest = updated
        comments = (
            _item_comments(session, project_url, kind, int(item["iid"])) if include_comments else []
        )
        yield _item_to_row(item, kind, project, comments, max_content_chars)
        changed += 1

    # An empty sweep while items were previously known almost always means a
    # failed listing, a renamed project, or a token that lost access — not a
    # genuine wipe. Treating it as "all deleted" would purge the dataset and
    # overwrite known_ids with [], making the loss permanent. Skip deletion.
    if known_ids and not current_ids:
        logger.warning(
            "GitLab: %s sweep returned 0 items but %d were known; skipping deletion "
            "this run to avoid a mass forget-on-delete on a transient sweep.",
            label,
            len(known_ids),
        )
        state["last_updated"] = newest
        logger.info("GitLab: %s: %d changed, 0 deleted.", label, changed)
        return

    deleted = known_ids - current_ids
    for item_id in sorted(deleted):
        yield _deleted_row(item_id)

    state["known_ids"] = sorted(current_ids)
    state["last_updated"] = newest
    logger.info("GitLab: %s: %d changed, %d deleted.", label, changed, len(deleted))


# ---------------------------------------------------------------------------
# Public factory
# ---------------------------------------------------------------------------
def gitlab_source(
    *,
    project: str | None = None,
    base_url: str | None = None,
    token: str | None = None,
    kinds: tuple[str, ...] | list[str] = ("issues", "merge_requests"),
    include_comments: bool = True,
    max_content_chars: int | None = None,
    session: Any = None,
):
    """Return a ``dlt`` source that yields GitLab issues / merge requests.

    Args:
        project: Project path (``group/name``) or numeric id. Falls back to
            ``GITLAB_PROJECT``.
        base_url: Instance URL. Falls back to ``GITLAB_URL``, then gitlab.com.
        token: Personal access token (``read_api``). Falls back to
            ``GITLAB_TOKEN``. Optional for public projects, but comments need it.
        kinds: Any of ``"issues"``, ``"merge_requests"``.
        include_comments: Fold each item's non-system notes into its text.
        max_content_chars: Cap on one rendered document; comments beyond it are
            dropped with a count. Falls back to ``GITLAB_MAX_CONTENT_CHARS``,
            then 32,000. ``0`` disables the cap.
        session: Pre-built ``requests`` session. Mainly an injection point for
            tests; when omitted one is built from ``token``.

    Returns:
        A ``dlt`` source named ``gitlab`` with one resource per kind
        (``gitlab_issues``, ``gitlab_merge_requests``), each configured with
        ``primary_key="id"``, ``write_disposition="merge"`` and a ``_deleted``
        hard-delete column, tagged as a cognee document source with its own
        pipeline scope.
    """
    project = project or os.environ.get("GITLAB_PROJECT")
    if not project:
        raise ValueError("gitlab_source requires project= or GITLAB_PROJECT.")
    base_url = (base_url or os.environ.get("GITLAB_URL") or DEFAULT_BASE_URL).rstrip("/")
    token = token or os.environ.get("GITLAB_TOKEN")
    if max_content_chars is None:
        max_content_chars = int(
            os.environ.get("GITLAB_MAX_CONTENT_CHARS") or DEFAULT_MAX_CONTENT_CHARS
        )
    if max_content_chars < 0:
        raise ValueError("max_content_chars must be 0 (no cap) or a positive number.")
    unknown = [k for k in kinds if k not in KINDS]
    if unknown:
        raise ValueError(f"unknown kinds {unknown}; expected a subset of {sorted(KINDS)}")

    def _make_resource(kind: str):
        table = KINDS[kind].table

        @dlt.resource(
            name=table,
            primary_key="id",
            write_disposition="merge",
            # _deleted is a boolean hard-delete marker: rows where it is True
            # are removed from the dlt destination on merge, which propagates
            # the deletion through cognee's orphan_cleanup.
            columns={"_deleted": {"data_type": "bool", "hard_delete": True}},
        )
        def _items():
            client = session or _make_session(token)
            yield from sync_items(
                client,
                base_url,
                project,
                kind,
                dlt.current.resource_state(),
                include_comments=include_comments,
                max_content_chars=max_content_chars,
            )

        return _items

    @dlt.source(name=GITLAB_SOURCE_NAME)
    def _gitlab():
        return [_make_resource(kind) for kind in kinds]

    source = _gitlab()
    # Opt into the document ingestion path (item → text document → cognify).
    # resolve_dlt_sources reads this marker; it never imports this connector.
    setattr(source, DOCUMENT_SOURCE_ATTR, GITLAB_SOURCE_NAME)
    # Own dlt pipeline per (dataset, instance, project): cognee derives the pipeline
    # name from this scope, so the cursor and known-id set of this project cannot be
    # overwritten by another dlt source or another project run in between.
    setattr(source, PIPELINE_SCOPE_ATTR, f"{GITLAB_SOURCE_NAME}:{base_url}:{project}")
    return source
