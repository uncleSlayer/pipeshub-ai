#!/usr/bin/env python3
"""Project enterprise commits onto the public repository.

Run from a non-bare clone of the enterprise repository that has the public
repository configured as a remote.

    ./sync.py --align --push     # once, after reconcile.py reports clean
    ./sync.py --push             # daily

Each published commit carries an `EE-Source:` trailer, so the public history is
its own bookmark -- there is no state to persist on the runner. The public
branch is only ever fast-forwarded: a commit on it that the sync did not
produce stops the run instead of being reverted.
"""

from __future__ import annotations

import argparse
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from projection import (  # noqa: E402
    DEFAULT_RULES,
    TRAILER,
    Rules,
    SyncError,
    changed_paths,
    check_tree,
    find_last_published,
    git,
    git_ok,
    project_tree,
    unmerged_public_work,
)


def resolve(repo: str, rev: str) -> str | None:
    if not git_ok(["rev-parse", "--verify", "--quiet", f"{rev}^{{commit}}"], repo=repo):
        return None
    return git(["rev-parse", f"{rev}^{{commit}}"], repo=repo).strip()


def tree_of(repo: str, commit: str) -> str:
    return git(["rev-parse", f"{commit}^{{tree}}"], repo=repo).strip()


def commit_meta(repo: str, commit: str) -> dict[str, str]:
    raw = git(["show", "-s", "--format=%an%x00%ae%x00%aI%x00%B", commit], repo=repo)
    name, email, date, body = raw.split("\0", 3)
    return {"name": name, "email": email, "date": date, "body": body.strip()}


def bot_identity(committer: str, date: str, subject: str) -> dict[str, str]:
    name, _, email = committer.partition("<")
    return {"name": name.strip(), "email": email.rstrip(">").strip(), "date": date, "body": subject}


def create_commit(repo: str, tree: str, parent: str | None, meta: dict[str, str],
                  message: str, committer: str) -> str:
    name, _, email = committer.partition("<")
    env = {
        "GIT_AUTHOR_NAME": meta["name"],
        "GIT_AUTHOR_EMAIL": meta["email"],
        "GIT_AUTHOR_DATE": meta["date"],
        "GIT_COMMITTER_NAME": name.strip(),
        "GIT_COMMITTER_EMAIL": email.rstrip(">").strip(),
        "GIT_COMMITTER_DATE": meta["date"],
    }
    args = ["commit-tree", tree] + (["-p", parent] if parent else [])
    return git(args, repo=repo, env=env, stdin=f"{meta['body']}\n\n{TRAILER}: {message}\n").strip()


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--repo", default=".", help="enterprise clone (non-bare)")
    parser.add_argument("--rules", type=Path, default=DEFAULT_RULES)
    parser.add_argument("--oss-remote", default="oss", help="git remote for the public repo")
    parser.add_argument("--push", action="store_true", help="push; otherwise dry-run")
    parser.add_argument(
        "--align", action="store_true",
        help="bootstrap an unsynced public repo with a single catch-up commit",
    )
    parser.add_argument(
        "--adopt", action="store_true",
        help="when the public branch has moved on its own, build on top of it and "
             "publish one catch-up commit -- permitted only once the enterprise tree "
             "already contains that public work",
    )
    parser.add_argument("--limit", type=int, default=0, help="stop after N projected commits")
    parser.add_argument("--committer", default="pipeshub-sync <noreply@pipeshub.com>")
    args = parser.parse_args()

    repo, rules = args.repo, Rules.load(args.rules)

    source_head = resolve(repo, rules.source_branch)
    if source_head is None:
        raise SyncError(f"source branch {rules.source_branch!r} not found in {repo}")

    git(["fetch", "--quiet", args.oss_remote,
         f"+refs/heads/{rules.target_branch}:refs/remotes/{args.oss_remote}/{rules.target_branch}"],
        repo=repo)
    oss_ref = f"refs/remotes/{args.oss_remote}/{rules.target_branch}"
    oss_head = resolve(repo, oss_ref)
    if oss_head is None:
        raise SyncError(f"{args.oss_remote}/{rules.target_branch} not found")

    published_at, last_source = find_last_published(repo, oss_ref)
    if published_at is None and not args.align:
        raise SyncError(
            f"no {TRAILER} trailer anywhere in {args.oss_remote}/{rules.target_branch}; "
            "this repo has never been synced -- run reconcile.py, then --align"
        )
    if args.align and published_at is not None:
        raise SyncError(
            f"{args.oss_remote}/{rules.target_branch} is already synced "
            f"(from {last_source[:12]}); drop --align"
        )
    if last_source and resolve(repo, last_source) is None:
        raise SyncError(
            f"public head references enterprise commit {last_source[:12]}, which is not "
            "in this clone -- fetch the enterprise repo, or history was rewritten"
        )

    drift = [c for c in git(["rev-list", f"{published_at}..{oss_head}"], repo=repo).splitlines() if c] \
        if published_at else []

    index_path = tempfile.mktemp(prefix="oss-sync-index-")
    collapse = args.align
    try:
        if drift:
            if not args.adopt:
                listing = git(["log", "--format=  %h  %an  %s", f"{published_at}..{oss_head}"],
                              repo=repo).rstrip()
                print(
                    "The public branch has commits the sync did not produce:\n"
                    f"{listing}\n\n"
                    "Publishing now would revert them. Land each one in the enterprise\n"
                    "repo (git cherry-pick -x <sha>), then re-run with --adopt: the sync\n"
                    "verifies the enterprise tree really has that work before continuing.",
                    file=sys.stderr,
                )
                return 2
            projected, _ = project_tree(repo, source_head, rules, index_path)
            lost = unmerged_public_work(repo, published_at, oss_head, projected)
            if lost:
                print(f"refusing to adopt: the enterprise tree still lacks public work on "
                      f"{len(lost)} path(s):", file=sys.stderr)
                for path in lost[:40]:
                    print(f"  {path}", file=sys.stderr)
                if len(lost) > 40:
                    print(f"  ... and {len(lost) - 40} more", file=sys.stderr)
                print("\nrun reconcile.py --emit-patch to land them first", file=sys.stderr)
                return 2
            print(f"adopting {len(drift)} public commit(s); publishing one catch-up commit")
            collapse = True

        todo = [source_head] if collapse else [
            c for c in git(
                ["rev-list"] + (["--first-parent"] if rules.first_parent else [])
                + ["--reverse", f"{last_source}..{source_head}"], repo=repo
            ).splitlines() if c
        ]
        if not todo:
            print("nothing to sync; the public branch already matches the enterprise source")
            return 0

        head, published = oss_head, 0
        for ee_sha in todo:
            tree, excluded = project_tree(repo, ee_sha, rules, index_path)
            if tree == tree_of(repo, head):
                continue

            violations = check_tree(
                repo, tree, rules, changed_paths(repo, tree_of(repo, head), tree)
            )
            if violations:
                print(f"refusing to publish {ee_sha[:12]} -- {len(violations)} guard "
                      f"violation(s):", file=sys.stderr)
                for item in violations[:50]:
                    print(f"  {item}", file=sys.stderr)
                if len(violations) > 50:
                    print(f"  ... and {len(violations) - 50} more", file=sys.stderr)
                return 3

            meta = commit_meta(repo, ee_sha)
            if collapse:
                # A catch-up commit squashes many authors' work; attributing it to
                # whoever happened to author the tip commit would misrepresent it.
                meta = bot_identity(args.committer, meta["date"],
                                    "chore(sync): align public tree with enterprise source")
            head = create_commit(repo, tree, head, meta, ee_sha, args.committer)
            published += 1
            subject = (meta["body"].splitlines() or ["(no subject)"])[0]
            print(f"  {ee_sha[:12]} -> {head[:12]}  ({len(excluded)} stripped)  {subject[:64]}")
            if args.limit and published >= args.limit:
                print(f"stopping at --limit {args.limit}")
                break
    finally:
        Path(index_path).unlink(missing_ok=True)

    if published == 0:
        print("no public-visible changes in this range")
        return 0
    if not args.push:
        print(f"\ndry run: would push {published} commit(s) to "
              f"{args.oss_remote}/{rules.target_branch} (re-run with --push)")
        return 0

    git(["push", args.oss_remote, f"{head}:refs/heads/{rules.target_branch}"], repo=repo)
    print(f"pushed {published} commit(s) to {args.oss_remote}/{rules.target_branch}")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except SyncError as err:
        print(f"error: {err}", file=sys.stderr)
        sys.exit(1)
