# Repository Agent Contract

## Mission

Own tokenized-asset and stablecoin issuer/on-chain evidence for this repository. Produce reproducible issuer observations, contract evidence, raw-object provenance and derived public views without treating partial coverage as the global tokenized-asset market.

## Canonical authority

- Prefer issuer primary disclosures and finalized/public-chain contract evidence appropriate to each asset.
- Preserve asset/contract/issuer identity, block or observation reference, retrieval time, source URL, source hash and raw evidence path required by the owning schema.
- Raw evidence bytes and published provenance must remain cryptographically linked where the current contract requires SHA-256.
- Other finance repositories should reference versioned artifacts here instead of copying tokenized-asset observations into parallel authorities.

## Autonomous execution

1. Inspect current `main`, README, open Issues/PRs, evidence objects/manifests, workflows, tests and public API before choosing work.
2. Continue one canonical workline for the same outcome before adding alternate RPC collectors, ledgers or views.
3. Prefer new verified primary/on-chain evidence, provenance integrity, deterministic regeneration, public read-back, or simplification of recurring collection work.
4. Keep live collection separate from deterministic PR/offline verification when upstream RPC reliability would otherwise make review non-reproducible.
5. Run the smallest relevant provenance/data tests and exact-revision checks before merge.
6. Stop when the bounded evidence/capability is verified; do not expand asset coverage solely to create activity.

## Branch lifecycle

- Aside from the default branch and unavoidable platform-managed/protected branches, a persistent branch is permitted only while it is the head branch of a currently open PR.
- Creating a work branch creates an obligation to open or reuse its canonical PR immediately; do not use branches as backlog, continuation state, backup, archive, or evidence storage.
- After a PR is merged or closed, delete its head branch after verifying PR/main state. A branch with no open PR is an orphan and must be deleted.
- Before and after work, compare repository branches with open PR heads. Do not report cleanup/fixed point while an orphan task branch remains.
- If the available tool cannot delete a branch, record that as a tooling blocker and do not claim cleanup complete. Never create another orphan branch as a workaround.

## Merge and release are separate

### PR merge conditions

A PR may merge when deterministic repository-local evidence is correct on the exact reviewed revision: schema/provenance/hash contracts hold, offline regeneration succeeds where affected, focused tests pass, and no unresolved review or correctness blocker remains.

Live RPC success, a future chain observation, external endpoint availability, published API state, or production deployment is **not** a merge condition unless the PR specifically changes the release/live-collection mechanism and that mechanism must be exercised before merge. An unavailable live endpoint must not force deterministic repository-local work to remain unmerged.

### Product/data release conditions

Release is a separate post-merge decision. Treat a tokenized-asset data/API release as complete only after the merged `main` revision is read back and the release surfaces in scope are actually verified, including live collection when required, published provenance/hash binding, API/deployment identity, and rollback/rebuild path where applicable.

A merged PR does not prove live collection or public release. A release/live-source blocker does not retroactively invalidate a correctly merged deterministic change. Report merge and release separately.

## Boundaries

- A small tracked asset set is not a proxy for total global tokenized-asset market value.
- Missing issuer/on-chain fields remain missing; do not extrapolate supply, market size, yield or adoption.
- Do not silently replace finalized evidence with an unverified endpoint result.
- Do not execute token transfers, swaps, approvals, wallet operations, trades or account actions.
- Unobserved live RPC, CI or deployment layers remain unverified.

## Completion report

Report verified evidence/coverage Before -> After, canonical hashes/artifacts, Issue/PR/commit/check evidence, then report `merged` and `released` separately with direct evidence for each. Include branch cleanup state, duplicate collectors/manual work removed and the real remaining blocker.

## Outcome-first task interpretation (Altitude / Depth / Range / Speed)

- **Altitude:** Interpret a task as a verifiable user outcome, not only the named intermediate step. Infer the smallest justified acceptance criteria from the request and current repository state; do not silently expand scope or override explicit instructions.
- **Depth:** Use primary/current evidence and real domain tools and methods. Verify the actual postcondition independently of agent self-report, preferably on the exact changed revision or target runtime.
- **Range:** Combine code, data, tests, runtime/UI, and domain expertise when that materially improves the outcome, while respecting canonical owners and avoiding duplicate systems.
- **Speed:** Complete authorized read-only and reversible work through implementation, focused checks, and read-back without unnecessary approval loops. Bound retries and stop at the proven outcome.
- These principles never bypass repository-specific security, privacy, permission, publication, spending, destructive-action, or release boundaries. State verified results and any remaining `UNVERIFIED` layers.
