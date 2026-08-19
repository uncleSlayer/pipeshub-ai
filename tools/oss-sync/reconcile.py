#!/usr/bin/env python3
"""Report how far the public repo has drifted from the enterprise projection.

Run this before the first sync, and any time the sync halts on drift. It
answers the only question that matters at that moment: *what would the first
projection destroy?*

    ./reconcile.py --oss-remote oss
    ./reconcile.py --oss-remote oss --emit-patch /tmp/backport.patch

Every differing path is classified three ways against the merge base, because
"the trees differ" is not actionable on its own -- what matters is which side
moved. Only paths the public side moved need a backport; paths where the
enterprise tree is simply ahead are what the sync is for.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
import tempfile
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from projection import (  # noqa: E402
    DEFAULT_RULES,
    NEEDS_ACTION,
    find_last_published,
    Rules,
    SyncError,
    blob_map,
    classify,
    git,
    git_ok,
    project_tree,
)


def project(repo: str, commit: str, rules: Rules) -> tuple[str, int]:
    index_path = tempfile.mktemp(prefix="oss-reconcile-index-")
    try:
        tree, excluded = project_tree(repo, commit, rules, index_path)
    finally:
        Path(index_path).unlink(missing_ok=True)
    return tree, len(excluded)


def touched_by(repo: str, oss_ref: str, base_commit: str | None, paths: list[str]) -> dict[str, list[str]]:
    owners: dict[str, list[str]] = defaultdict(list)
    if not paths or base_commit is None:
        return owners
    raw = git(
        ["log", "--format=%x01%h %an", "--name-only", f"{base_commit}..{oss_ref}", "--", *paths],
        repo=repo,
        check=False,
    )
    current = ""
    for line in raw.splitlines():
        if line.startswith("\x01"):
            current = line[1:]
        elif line.strip() and current:
            owners[line.strip()].append(current)
    return owners


CATEGORIES = [
    ("public_ahead", "PUBLIC AHEAD", "public work the enterprise tree lacks -- BACKPORT or it is lost"),
    ("both_moved", "BOTH MOVED", "both sides edited since the fork -- resolve by hand"),
    ("unknown", "UNRESOLVED", "no common ancestor; compare by hand"),
    ("ee_ahead", "ENTERPRISE AHEAD", "will be published as-is (expected)"),
    ("ee_deleted", "REMOVED BY ENTERPRISE", "will be deleted from the public repo"),
]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--repo", default=".")
    parser.add_argument("--rules", type=Path, default=DEFAULT_RULES)
    parser.add_argument("--oss-remote", default="oss")
    parser.add_argument("--emit-patch", type=Path, help="write a public -> enterprise backport patch")
    parser.add_argument("--limit", type=int, default=40, help="paths listed per category")
    args = parser.parse_args()

    repo, rules = args.repo, Rules.load(args.rules)
    git(
        ["fetch", "--quiet", args.oss_remote,
         f"+refs/heads/{rules.target_branch}:refs/remotes/{args.oss_remote}/{rules.target_branch}"],
        repo=repo,
    )
    oss_ref = f"refs/remotes/{args.oss_remote}/{rules.target_branch}"
    source_head = git(["rev-parse", f"{rules.source_branch}^{{commit}}"], repo=repo).strip()

    projected, stripped = project(repo, source_head, rules)
    public = blob_map(repo, oss_ref)
    ee = blob_map(repo, projected)

    # The newest commit the sync produced is the last point at which the two
    # trees agreed, which is a far sharper baseline than the original fork
    # point -- against merge-base every file ever published looks like both
    # sides moved.
    published_at, _ = find_last_published(repo, oss_ref)
    if published_at:
        base_commit = published_at
        base_label = f"last published  {base_commit[:12]}"
        base = blob_map(repo, base_commit)
    else:
        base_commit = git(["merge-base", oss_ref, source_head], repo=repo, check=False).strip() or None
        base_label = f"merge base      {base_commit[:12]}" if base_commit else "merge base      (none -- unrelated histories)"
        base = blob_map(repo, project(repo, base_commit, rules)[0]) if base_commit else None

    print(f"enterprise {rules.source_branch}  {source_head[:12]}   ({stripped} paths stripped)")
    print(f"public     {rules.target_branch}  {git(['rev-parse', oss_ref], repo=repo).strip()[:12]}")
    print(base_label)

    buckets = classify(base, public, ee)
    agreed = len({p for p in set(public) & set(ee) if public[p] == ee[p]})
    print(f"\n  in agreement   {agreed:>6}")
    for key, label, _ in CATEGORIES:
        if buckets.get(key):
            print(f"  {label:<22} {len(buckets[key]):>5}")

    owners = touched_by(repo, oss_ref, base_commit, [p for k in NEEDS_ACTION for p in buckets.get(k, [])])
    for key, label, blurb in CATEGORIES:
        group = buckets.get(key)
        if not group:
            continue
        print(f"\n--- {label} ({len(group)}) -- {blurb}")
        for path in group[: args.limit]:
            who = ", ".join(dict.fromkeys(owners.get(path, [])))
            print(f"  {path}" + (f"    [{who}]" if who else ""))
        if len(group) > args.limit:
            print(f"  ... and {len(group) - args.limit} more (raise --limit)")

    actionable = [p for key in NEEDS_ACTION for p in buckets.get(key, [])]

    if args.emit_patch and actionable:
        with open(args.emit_patch, "w") as handle:
            proc = subprocess.run(
                ["git", "-C", repo, "diff", projected, oss_ref, "--", *sorted(actionable)],
                stdout=handle, stderr=subprocess.PIPE, text=True,
            )
        if proc.returncode != 0:
            raise SyncError(f"git diff failed: {proc.stderr.strip()}")
        print(f"\nwrote backport patch ({len(actionable)} paths): {args.emit_patch}")
        print(f"apply inside the enterprise repo:  git apply --3way {args.emit_patch}")

    if actionable:
        print(
            f"\nNot safe to publish: {len(actionable)} path(s) carry public work the "
            "enterprise tree does not have.\nLand them in the enterprise repo, then "
            "re-run. Once this section is empty:  sync.py --align --push"
        )
        return 1

    print("\npublic tree carries no unmerged work -- safe to run: sync.py --align --push")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except SyncError as err:
        print(f"error: {err}", file=sys.stderr)
        sys.exit(2)
