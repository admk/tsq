Implement one small optimization attempt in the current worktree.

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

Configured hard change cap: $change_scope.

Rules:
- Work only in the current worktree and leave the repository in a reviewable state.
- Keep the action focused and reviewable, including when the configured cap is unlimited.
- Change implementation code only; do not modify tests, benchmarks, evaluators, fixtures, campaign state, or validation inputs.
- Do not run configured validation or GPU workloads; the dedicated validation phase evaluates the resulting commit.
- If the exact approach is blocked, make the safest useful bounded progress and explain the evidence in your final response.
