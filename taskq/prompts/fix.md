Assess the candidate and fix it when a bounded correction is needed.

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
- Inspect the current Git state, candidate diff, and any available trusted validation evidence before deciding.
- If the candidate is already useful, correct, and permitted by every hard gate, leave HEAD unchanged and accept it.
- If a focused correction can make the candidate acceptable, edit only the current worktree and leave it in a clean, committed, reviewable state.
- If `edits_allowed` is false in the artifacts, do not edit; choose accept, abandon, or stop with HEAD unchanged.
- Any edit forces configured validation and a fresh fix pass. An agent that edits cannot accept its own changes, and taskq ignores its response decision.
- Never modify protected inputs, tests, benchmarks, evaluators, fixtures, campaign state, or validation inputs.
- Do not run configured validation or GPU workloads; the dedicated validation phase evaluates edited commits.
- Keep every correction focused and within the configured cumulative change cap, including when that cap is unlimited.
- Do not ask the user questions.
- If HEAD is unchanged, end with one single-line JSON object in this exact shape: TASKQ_JSON: {"decision":"accept","reason":"...","evidence":["artifact-id"],"memory_updates":[],"next_direction":null}
  - decision: exactly one decision name below:
    - accept: HEAD was not changed and the candidate is ready for integration.
    - abandon: with HEAD unchanged, the approach is invalid, unsafe, unhelpful, or not worth another fix.
    - stop: with HEAD unchanged, abandon this candidate and stop exploring after accepted work drains.
  - reason: a concise justification grounded in the current diff and trusted evidence.
  - evidence: identifiers or short references to artifacts supporting the decision.
  - memory_updates: durable lessons for later directions, as strings or objects with a claim field.
  - next_direction: null unless proposing a structurally distinct follow-up direction object.
