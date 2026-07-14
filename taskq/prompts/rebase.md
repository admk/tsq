Resolve the in-progress rebase in the current attempt worktree.

Objective:
$objective

Direction:
$direction

Relevant memory:
$memory

Artifacts and evidence:
$artifacts

Additional context:
$context

Configured change scope: $change_scope.

Rules:
- Treat the latest campaign mainline as the parent whose behavior has highest priority.
- First preserve the parent behavior without regressions or reversals; only then preserve as much of the accepted optimization behavior as remains compatible.
- Inspect Git status and resolve only the current conflicts. Do not create a merge commit or introduce unrelated changes.
- Complete the rebase only when both priorities can be satisfied safely.
- If the optimization cannot be integrated without changing parent behavior, abort the rebase and clearly explain the incompatibility and abandonment reason in the final response.
- Do not weaken tests, benchmarks, evaluators, fixtures, or campaign state.
- Do not ask the user questions; make the safest resolution supported by the repository and artifacts.
