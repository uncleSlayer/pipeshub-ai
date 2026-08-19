#!/usr/bin/env python3
"""Plumbing for projecting the enterprise tree onto its public counterpart.

The projection is a pure function of an enterprise commit: take that commit's
tree, drop every path the publish rules exclude, and the result is exactly what
the OSS repository should contain. Because nothing is ever merged in the public
direction, the OSS tree cannot conflict with the enterprise tree -- it is
derived from it.
"""

from __future__ import annotations

import os
import re
import subprocess
import tomllib
from dataclasses import dataclass
from pathlib import Path

DEFAULT_RULES = Path(__file__).resolve().parent / "publish.toml"

# `git grep` and `git update-index` take paths on argv; chunk to stay well
# under ARG_MAX on trees with tens of thousands of files.
_ARG_CHUNK = 400


class SyncError(RuntimeError):
    """Any condition that must stop a sync rather than publish something."""


def git(
    args: list[str],
    *,
    repo: str = ".",
    env: dict[str, str] | None = None,
    stdin: str | None = None,
    check: bool = True,
) -> str:
    full_env = {**os.environ, **(env or {})}
    proc = subprocess.run(
        ["git", "-C", repo, *args],
        env=full_env,
        input=stdin,
        capture_output=True,
        text=True,
    )
    if check and proc.returncode != 0:
        raise SyncError(
            f"git {' '.join(args)} failed ({proc.returncode}): {proc.stderr.strip()}"
        )
    return proc.stdout


def git_ok(args: list[str], *, repo: str = ".") -> bool:
    return subprocess.run(
        ["git", "-C", repo, *args], capture_output=True, text=True
    ).returncode == 0


def _nul_list(payload: str) -> list[str]:
    return [item for item in payload.split("\0") if item]


def _glob_to_regex(pattern: str) -> re.Pattern[str]:
    out: list[str] = []
    i, n = 0, len(pattern)
    while i < n:
        if pattern.startswith("**/", i):
            out.append("(?:[^/]+/)*")
            i += 3
        elif pattern.startswith("**", i):
            out.append(".*")
            i += 2
        elif pattern[i] == "*":
            out.append("[^/]*")
            i += 1
        elif pattern[i] == "?":
            out.append("[^/]")
            i += 1
        else:
            out.append(re.escape(pattern[i]))
            i += 1
    return re.compile("".join(out) + r"\Z")


@dataclass(frozen=True)
class ContentGuard:
    name: str
    pattern: str


@dataclass
class Rules:
    source_branch: str
    first_parent: bool
    target_branch: str
    exclude: list[re.Pattern[str]]
    allowed_top_level: set[str]
    forbidden_path_segments: set[str]
    forbidden_content: list[ContentGuard]

    @classmethod
    def load(cls, path: Path) -> "Rules":
        with open(path, "rb") as handle:
            raw = tomllib.load(handle)
        source = raw.get("source", {})
        target = raw.get("target", {})
        guards = raw.get("guards", {})
        return cls(
            source_branch=source.get("branch", "master"),
            first_parent=bool(source.get("first_parent", True)),
            target_branch=target.get("branch", "main"),
            exclude=[_glob_to_regex(p) for p in raw.get("exclude", {}).get("paths", [])],
            allowed_top_level=set(raw.get("surface", {}).get("top_level", [])),
            forbidden_path_segments=set(guards.get("forbidden_path_segments", [])),
            forbidden_content=[
                ContentGuard(name=item["name"], pattern=item["pattern"])
                for item in guards.get("forbidden_content", [])
            ],
        )

    def _matches(self, path: str) -> bool:
        return any(rx.match(path) for rx in self.exclude)

    def partition(self, paths: list[str]) -> tuple[list[str], list[str]]:
        """Split repo paths into (published, excluded).

        Directories are resolved once and cached, so a pattern like `**/ee`
        excludes every descendant without re-testing each file against it.
        """
        dir_verdict: dict[str, bool] = {"": False}

        def dir_excluded(directory: str) -> bool:
            if directory in dir_verdict:
                return dir_verdict[directory]
            parent = directory.rpartition("/")[0]
            verdict = dir_excluded(parent) or self._matches(directory)
            dir_verdict[directory] = verdict
            return verdict

        published, excluded = [], []
        for path in paths:
            parent = path.rpartition("/")[0]
            (excluded if dir_excluded(parent) or self._matches(path) else published).append(path)
        return published, excluded


def list_tree(repo: str, treeish: str) -> list[str]:
    return _nul_list(git(["ls-tree", "-r", "-z", "--name-only", treeish], repo=repo))


def project_tree(repo: str, commit: str, rules: Rules, index_path: str) -> tuple[str, list[str]]:
    """Write the projected tree object for `commit`; return (tree_sha, excluded)."""
    env = {"GIT_INDEX_FILE": index_path}
    published, excluded = rules.partition(list_tree(repo, commit))
    git(["read-tree", commit], repo=repo, env=env)
    if excluded:
        git(
            ["update-index", "--force-remove", "-z", "--stdin"],
            repo=repo,
            env=env,
            stdin="\0".join(excluded) + "\0",
        )
    if not published:
        raise SyncError(f"projection of {commit} is empty; refusing to publish")
    return git(["write-tree"], repo=repo, env=env).strip(), excluded


def changed_paths(repo: str, before_tree: str | None, after_tree: str) -> list[str]:
    if before_tree is None:
        return list_tree(repo, after_tree)
    payload = git(
        ["diff-tree", "-r", "-z", "--name-only", "--diff-filter=ACMRT", before_tree, after_tree],
        repo=repo,
    )
    return _nul_list(payload)


def _grep_tree(repo: str, tree: str, pattern: str, paths: list[str]) -> list[str]:
    hits: list[str] = []
    for start in range(0, len(paths), _ARG_CHUNK):
        chunk = paths[start : start + _ARG_CHUNK]
        proc = subprocess.run(
            ["git", "-C", repo, "grep", "-I", "-n", "-E", "-e", pattern, tree, "--", *chunk],
            capture_output=True,
            text=True,
        )
        if proc.returncode not in (0, 1):
            raise SyncError(f"git grep failed: {proc.stderr.strip()}")
        hits.extend(line for line in proc.stdout.splitlines() if line)
    return hits


def check_tree(repo: str, tree: str, rules: Rules, scan_paths: list[str]) -> list[str]:
    """Return human-readable violations; empty means the tree is safe to publish."""
    violations: list[str] = []

    top_level = {
        name for name in git(["ls-tree", "--name-only", tree], repo=repo).splitlines() if name
    }
    for unexpected in sorted(top_level - rules.allowed_top_level):
        violations.append(
            f"surface: unexpected top-level entry {unexpected!r} -- add it to "
            f"[surface].top_level if it is public, or to [exclude].paths if it is not"
        )

    if rules.forbidden_path_segments:
        for path in list_tree(repo, tree):
            segments = set(path.split("/"))
            leaked = segments & rules.forbidden_path_segments
            if leaked:
                violations.append(f"path: {path} contains forbidden segment {sorted(leaked)[0]!r}")

    if scan_paths:
        for guard in rules.forbidden_content:
            for hit in _grep_tree(repo, tree, guard.pattern, scan_paths):
                violations.append(f"content[{guard.name}]: {hit}")

    return violations


def blob_map(repo: str, treeish: str) -> dict[str, str]:
    out: dict[str, str] = {}
    for entry in git(["ls-tree", "-r", "-z", treeish], repo=repo).split("\0"):
        if not entry:
            continue
        meta, _, path = entry.partition("\t")
        out[path] = meta.split()[2]
    return out


# Buckets whose presence means the public repo holds work that publishing would
# destroy. Everything else is either agreement or the enterprise tree moving
# forward, which is the whole point of the sync.
NEEDS_ACTION = ("public_ahead", "both_moved", "unknown")


def classify(
    base: dict[str, str] | None, public: dict[str, str], ee: dict[str, str]
) -> dict[str, list[str]]:
    """Bucket every path on which the public tree and the projection disagree.

    `base` is the last state at which the two agreed. Without it there is no way
    to tell a public contribution apart from an enterprise change, so those
    paths land in `unknown` rather than being guessed at.
    """
    buckets: dict[str, list[str]] = {}

    def add(key: str, path: str) -> None:
        buckets.setdefault(key, []).append(path)

    for path in set(public) | set(ee):
        pub, ent = public.get(path), ee.get(path)
        if pub == ent:
            continue
        if base is None:
            add("unknown" if pub and ent else ("public_ahead" if ent is None else "ee_ahead"), path)
            continue
        old = base.get(path)
        if pub == old:
            add("ee_ahead" if ent is not None else "ee_deleted", path)
        elif ent == old:
            add("public_ahead", path)
        else:
            add("both_moved", path)
    return {key: sorted(value) for key, value in buckets.items()}


def unmerged_public_work(
    repo: str, mirror: str, oss_head: str, projected_tree: str
) -> list[str]:
    """Paths the public repo would lose if `projected_tree` were published now."""
    buckets = classify(
        blob_map(repo, mirror), blob_map(repo, oss_head), blob_map(repo, projected_tree)
    )
    return sorted(path for key in NEEDS_ACTION for path in buckets.get(key, []))


TRAILER = "EE-Source"


def find_last_published(repo: str, oss_ref: str, limit: int = 5000) -> tuple[str | None, str | None]:
    """Locate the newest public commit this sync produced, via its source trailer.

    The public history is the state file. Nothing has to be persisted in the
    enterprise repo or on a CI runner, and the sync can be rebuilt from any
    fresh clone of the two repositories.
    """
    raw = git(["log", "--format=%H%x00%B%x02", "-n", str(limit), oss_ref], repo=repo)
    for record in raw.split("\x02"):
        record = record.strip("\n")
        if not record:
            continue
        sha, _, body = record.partition("\0")
        for line in reversed(body.splitlines()):
            if line.startswith(f"{TRAILER}:"):
                return sha.strip(), line.split(":", 1)[1].strip()
    return None, None
