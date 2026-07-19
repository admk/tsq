Resolve the in-progress merge in the current attempt worktree.

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
- Treat the latest campaign mainline, the merge's first parent, as the behavior with highest priority.
- Preserve that parent behavior first, then preserve as much of the accepted optimization as remains compatible.
- Inspect Git status and resolve only the current aggregate merge conflicts.
- Stage every resolution with Git, but do not commit or abort the merge; taskq creates the merge commit after verifying that no conflicts remain.
- If the optimization cannot be integrated without changing parent behavior, leave the conflict unresolved and clearly explain the incompatibility in the final response.
- Do not introduce unrelated changes or weaken tests, benchmarks, evaluators, fixtures, or campaign state.
- Do not ask the user questions; make the safest resolution supported by the repository and artifacts.
