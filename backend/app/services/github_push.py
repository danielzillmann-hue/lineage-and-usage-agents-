"""Push a generated Dataform project to a GitHub repo.

The flow is intentionally minimal — git CLI in a temp dir, force-push so
re-runs always land cleanly. Auth is via a user-supplied PAT injected
into the remote URL. We never log or persist the token.
"""

from __future__ import annotations

import logging
import re
import shutil
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import quote

from app.services import gcs, transform_storage
from app.config import get_settings

log = logging.getLogger(__name__)


@dataclass
class PushResult:
    repo_url: str
    branch: str
    commit_sha: str
    commit_url: str
    files_pushed: int


class PushError(Exception):
    """Raised when any step of the git flow fails. Message is safe to
    surface to the user (token is redacted)."""


_REPO_URL_RE = re.compile(
    r"^https?://(?:www\.)?github\.com/(?P<owner>[\w.-]+)/(?P<repo>[\w.-]+?)(?:\.git)?/?$"
)


def push_to_github(
    run_id: str,
    repo_url: str,
    branch: str,
    commit_message: str,
    github_token: str,
    force: bool = True,
) -> PushResult:
    """Clone the run's transform output from GCS, push it to `repo_url`.

    Returns a PushResult with the commit SHA + a viewable URL on github.com.
    Raises PushError for any user-actionable failure (bad URL, auth, etc.).
    """
    m = _REPO_URL_RE.match(repo_url.strip())
    if not m:
        raise PushError(f"not a github.com URL: {repo_url}")
    owner, repo = m.group("owner"), m.group("repo")

    files = transform_storage.list_files(run_id)
    if not files:
        raise PushError(f"no transform output for run {run_id} — generate it first")

    # Build an authenticated remote URL. The token is URL-encoded in case
    # it contains characters git's URL parser would treat specially.
    auth_url = f"https://x-access-token:{quote(github_token, safe='')}@github.com/{owner}/{repo}.git"

    settings = get_settings()
    bucket = settings.results_bucket
    prefix = f"runs/{run_id}/transform"

    tmp = Path(tempfile.mkdtemp(prefix="dataform-push-"))
    try:
        # 1. Initialise local repo
        _git(tmp, "init", "-b", branch)
        _git(tmp, "config", "user.name", "intelia migration agent")
        _git(tmp, "config", "user.email", "noreply@intelia.com.au")

        # 2. Pull every file from GCS into the working dir.
        files_pushed = 0
        for path in files:
            content = gcs.read_text(bucket, f"{prefix}/{path}")
            target = tmp / path
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(content, encoding="utf-8")
            files_pushed += 1
        # Originals too — useful for code review on the PR.
        for orig in gcs.list_blobs(bucket, f"{prefix}/_originals/"):
            rel = orig.removeprefix(f"{prefix}/")
            content = gcs.read_text(bucket, orig)
            target = tmp / rel
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(content, encoding="utf-8")
            files_pushed += 1

        # 3. Commit
        _git(tmp, "add", ".")
        try:
            _git(tmp, "commit", "-m", commit_message)
        except PushError as e:
            # Empty diff is OK if user is force-pushing identical content.
            if "nothing to commit" not in str(e).lower():
                raise

        # 4. Push
        _git(tmp, "remote", "add", "origin", auth_url)
        push_args = ["push", "--force-with-lease" if not force else "--force",
                     "-u", "origin", branch]
        _git(tmp, *push_args)

        sha = _git(tmp, "rev-parse", "HEAD").strip()
        commit_url = f"https://github.com/{owner}/{repo}/commit/{sha}"

        return PushResult(
            repo_url=f"https://github.com/{owner}/{repo}",
            branch=branch,
            commit_sha=sha,
            commit_url=commit_url,
            files_pushed=files_pushed,
        )
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def _git(cwd: Path, *args: str) -> str:
    """Run git in `cwd`, return stdout. Redacts any embedded auth token
    from error messages so PATs never leak into logs or API responses.
    """
    result = subprocess.run(
        ["git", *args],
        cwd=str(cwd),
        capture_output=True,
        text=True,
        timeout=120,
    )
    if result.returncode != 0:
        msg = (result.stderr or result.stdout or "git failed").strip()
        # Strip any "x-access-token:<token>@github.com" pattern out of error text.
        msg = re.sub(r"x-access-token:[^@]+@", "x-access-token:***@", msg)
        raise PushError(f"git {args[0]}: {msg}")
    return result.stdout
