#!/usr/bin/env bash
# Exercises the projection against a throwaway pair of repositories that
# reproduce the real topology: an enterprise tree with ee/ overlays at several
# depths, edits to shared files, and a public repo carrying community commits
# the enterprise tree has never seen.
#
# This decides what becomes public, so it gets a test. Run it before changing
# publish.toml or anything under tools/oss-sync/.
set -euo pipefail

TOOLS="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
RULES="$WORK/rules.toml"
PASS=0

export GIT_AUTHOR_DATE="2026-01-01T00:00:00Z" GIT_COMMITTER_DATE="2026-01-01T00:00:00Z"
export GIT_CONFIG_GLOBAL=/dev/null GIT_CONFIG_SYSTEM=/dev/null

ok()   { printf '  ok    %s\n' "$1"; PASS=$((PASS+1)); }
fail() { printf '  FAIL  %s\n' "$1" >&2; exit 1; }
check(){ if eval "$2"; then ok "$1"; else fail "$1"; fi; }
gc()   { git -c user.name="$1" -c user.email="$2" commit -q -m "$3"; }
sync() { python3 "$TOOLS/sync.py" --rules "$RULES" --repo "$WORK/ee" --oss-remote oss "$@"; }
recon(){ python3 "$TOOLS/reconcile.py" --rules "$RULES" --repo "$WORK/ee" --oss-remote oss "$@"; }
# reconcile exits non-zero by design when it finds drift, which `pipefail` would
# otherwise turn into a failed assertion.
report(){ recon 2>/dev/null || true; }
section(){ report | awk -v want="$1" '/^--- /{on = index($0, want) == 5} on'; }

sed -e 's/^top_level = \[/top_level = [ "LICENSE", "README.md", "backend", "frontend",/' \
    "$TOOLS/publish.toml" > "$RULES"

cd "$WORK"
git init -q --bare oss.git
git init -q -b main pub && cd pub
mkdir -p backend/nodejs frontend
echo "export const version = '1.0';" > backend/nodejs/index.ts
echo "export const App = () => null;" > frontend/app.tsx
echo Apache-2.0 > LICENSE && echo "# PipesHub" > README.md
git add -A && gc Alice alice@pipeshub.com "initial public tree"
git remote add origin ../oss.git && git push -q origin main && cd ..

git clone -q pub ee && cd ee && git branch -q -m main master
mkdir -p ee/billing backend/nodejs/ee frontend/ee
echo "export const seats = 100;"     > ee/billing/seats.ts
echo "export const audit = true;"    > backend/nodejs/ee/audit.ts
echo "export const Sso = () => null;"> frontend/ee/sso.tsx
git add -A && gc Bob bob@pipeshub.com "feat(ee): billing, audit, sso"
echo "export const version = '1.1';" > backend/nodejs/index.ts
git add -A && gc Bob bob@pipeshub.com "fix(core): bump version"
git remote add oss ../oss.git && git fetch -q oss && cd ..

cd pub
echo "export const helper = 1;" > frontend/helper.ts
git add -A && gc Carol carol@example.org "feat: helper (#3001)"
git push -q origin main && cd ..

echo "== divergence detection =="
check "reconcile flags unmerged public work" '! recon >/dev/null 2>&1'
check "reconcile names the public-ahead path" 'report | grep -q "frontend/helper.ts"'
check "reconcile keeps enterprise-ahead files out of the backport list" \
      '! section "PUBLIC AHEAD" | grep -q "backend/nodejs/index.ts"'
check "reconcile lists the enterprise-ahead file as expected churn" \
      'section "ENTERPRISE AHEAD" | grep -q "backend/nodejs/index.ts"'
check "sync refuses before bootstrap" '! sync --push >/dev/null 2>&1'

cd ee && git fetch -q oss
git -c user.name=Bob -c user.email=bob@pipeshub.com cherry-pick -x \
    "$(git rev-parse refs/remotes/oss/main)" >/dev/null 2>&1
cd ..
check "reconcile clean once backported" 'recon >/dev/null 2>&1'

echo "== bootstrap =="
check "align dry-run pushes nothing" 'before=$(git -C oss.git rev-parse main); sync --align >/dev/null; [ "$(git -C oss.git rev-parse main)" = "$before" ]'
check "align --push succeeds" 'sync --align --push >/dev/null'
check "public tree has no ee/ paths" '! git -C ee ls-tree -r --name-only refs/remotes/oss/main | grep -qE "(^|/)ee(/|$)"'
check "public keeps community history" 'git -C ee log --format=%an refs/remotes/oss/main | grep -q Carol'
check "re-running is a no-op" 'sync --push 2>&1 | grep -q "nothing to sync"'

echo "== incremental projection =="
cd ee
export GIT_AUTHOR_DATE="2026-01-03T00:00:00Z" GIT_COMMITTER_DATE="2026-01-03T00:00:00Z"
echo "export const invoices = [];" > ee/billing/invoices.ts
git add -A && gc Bob bob@pipeshub.com "feat(ee): invoices"
echo "export const version = '1.2';" > backend/nodejs/index.ts
git add -A && gc Dave dave@pipeshub.com "fix(core): 1.2 (#3010)"
cd ..
check "sync publishes shared work" 'sync --push 2>&1 | grep -q "pushed 1 commit"'
check "enterprise-only commit never published" '! git -C ee log --format=%s refs/remotes/oss/main | grep -q "feat(ee): invoices"'
check "original author preserved" '[ "$(git -C ee log -1 --format=%an refs/remotes/oss/main)" = Dave ]'
check "source trailer recorded" 'git -C ee log -1 --format=%B refs/remotes/oss/main | grep -q "^EE-Source: "'

echo "== leak guards =="
guard_blocks() { # <setup-cmd> ; leaves the tree clean afterwards
  cd "$WORK/ee"; eval "$1"; git add -A; gc Bob bob@pipeshub.com "guard probe"
  local rc=0; sync --push >/dev/null 2>&1 || rc=$?
  git reset -q --hard HEAD~1; cd "$WORK"; [ "$rc" -eq 3 ]
}
check "blocks ts import reaching into ee/" \
      'guard_blocks "echo \"import { audit } from '\''./ee/audit'\'';\" > backend/nodejs/index.ts"'
check "blocks python import reaching into ee" \
      'guard_blocks "mkdir -p backend/python && echo \"from app.ee.licensing import check\" > backend/python/m.py"'
check "blocks an unlisted top-level directory" \
      'guard_blocks "mkdir -p enterprise-connectors && echo 1 > enterprise-connectors/sap.ts"'
check "public head untouched by blocked syncs" \
      '[ "$(git -C oss.git rev-parse main)" = "$(git -C ee rev-parse refs/remotes/oss/main)" ]'
check "sync is stateless -- no refs persisted in the enterprise repo" \
      '! git -C ee show-ref | grep -qE "oss-mirror|oss-sync/"'

echo "== drift handling =="
cd pub && git pull -q origin main
echo "export const fix = 1;" > backend/nodejs/patch.ts
git add -A && gc Frank frank@example.org "fix: XSS (#3020)"
git push -q origin main && cd ..
check "sync halts on an unexpected public commit" '! sync --push >/dev/null 2>&1'
check "adopt refuses while the work is unbackported" '! sync --adopt --push >/dev/null 2>&1'
cd ee && git fetch -q oss && git -c user.name=Bob -c user.email=bob@pipeshub.com \
    cherry-pick -x "$(git rev-parse refs/remotes/oss/main)" >/dev/null 2>&1; cd ..
check "adopt succeeds once backported" 'sync --adopt --push >/dev/null 2>&1'
check "adopt did not revert the public commit" 'git -C ee ls-tree -r --name-only refs/remotes/oss/main | grep -q "backend/nodejs/patch.ts"'
check "no ee/ path in ANY published commit" \
      '! for c in $(git -C ee rev-list refs/remotes/oss/main); do git -C ee ls-tree -r --name-only $c; done | grep -qE "(^|/)ee(/|$)"'

printf '\n%d checks passed\n' "$PASS"
