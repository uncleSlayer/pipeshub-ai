# oss-sync (prototype)

Publishes the enterprise tree to the public repository as a **projection**: the
public tree is `tree(enterprise commit)` with every path in `publish.toml`
stripped out. Nothing is merged in the public direction, so the two trees cannot
conflict — one is derived from the other.

> **Status: prototype, pending design decisions.** It is not wired into CI and
> has never run against the real repositories. See "Open questions" below — two
> of them may change the shape of this entirely.

## Why projection rather than merge

Merging `master` into the public `main` would put every `ee/` blob in that
history into the public object database permanently. Deleting the files
afterwards does not unpublish them: they stay reachable by SHA, GitHub serves
them across the fork network, and existing clones already have them. A
projection never writes those objects in the first place.

## Layout

| file | role |
| --- | --- |
| `publish.toml` | what is stripped, what public surface is allowed, leak guards |
| `projection.py` | tree projection, path matching, guards, drift classification |
| `sync.py` | daily job: project new commits, verify, fast-forward the public branch |
| `reconcile.py` | pre-flight: what would publishing destroy, and what to backport |
| `selftest.sh` | 25 assertions against throwaway repos reproducing the real topology |

## Usage

Run from a non-bare enterprise clone with the public repo as a remote:

```sh
git remote add oss git@github.com:pipeshub-ai/pipeshub-ai.git

./reconcile.py --oss-remote oss --emit-patch /tmp/backport.patch   # what diverged
./sync.py --align --push                                          # once, to bootstrap
./sync.py --push                                                  # daily
./sync.py --adopt --push                                          # after a public-side commit
```

`--push` is opt-in; without it everything is a dry run.

## Properties it holds

- **Append-only.** The public branch is only ever fast-forwarded, so community
  forks and open PRs stay valid. There is no force-push anywhere.
- **Attribution preserved.** Each public commit keeps its original author; the
  bot is only the committer.
- **Enterprise-only commits vanish.** Their projection is byte-identical to the
  previous one, so no empty commit is published.
- **Stateless.** Published commits carry an `EE-Source:` trailer, so the public
  history is its own bookmark — nothing to persist on a runner, and any clone
  can resume.
- **Fails closed.** An unlisted top-level directory, a shared file importing
  from `ee/`, or a commit on the public branch the sync did not produce all halt
  the run rather than publishing.

## Open questions that could change this

1. **Do shared files in the enterprise repo reference `ee/` directly?** If they
   do, the projection produces a repo that does not compile, and no sync tooling
   fixes that — it needs a seam (registry, dynamic import, entry points) that
   no-ops when `ee/` is absent. `guards.forbidden_content` in `publish.toml`
   detects the problem; it cannot solve it.
2. **How do community PRs land?** Every human commit on the public branch is
   drift. Today the sync halts and a human backports into the enterprise repo,
   then re-runs with `--adopt`. The alternative — public branch is bot-only,
   contributions are mirrored into the enterprise repo for review — is cleaner
   for the tooling and worse for contributors.
3. **Direction of flow.** This assumes enterprise → public. Public → enterprise
   is the conventional open-core arrangement and makes disclosure structurally
   impossible, at the cost of developers choosing a repo before they write code.

## Not built yet

- CI workflow (must build the projected tree before pushing — a projection that
  does not compile is worse than a stale mirror).
- Secret scanning on the projected tree.
- Automated backport PR from a merged public PR into the enterprise repo.
