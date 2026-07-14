Plan $direction_count distinct optimization work direction(s).

Objective:
$objective

Relevant memory:
$memory

Previously tried directions:
$tried_directions

Additional context:
$context

Configured change scope: $change_scope.

Rules:
- Do not edit the repository; propose bounded work only.
- Keep each direction small, focused, and reviewable.
- Prioritize directions that can execute concurrently: minimize overlap in files, assumptions, resources, and dependencies on other directions in this batch.
- Use prior evidence and make every direction structurally distinct from recorded successes, failures, and the other proposed directions.
- Do not ask the user questions; make the best safe assumption.
- Return exactly $direction_count direction(s).
- End with one single-line JSON object in this exact shape: TASKQ_JSON: {"directions":[{"title":"...","hypothesis":"...","approach":"...","success_signal":"...","different_from":["..."]}]}
  - title: a short human-readable label for the direction.
  - hypothesis: a falsifiable explanation of why this direction should improve the objective.
  - approach: the concrete, bounded implementation strategy to try.
  - success_signal: the observable check, metric, or evidence that would support the hypothesis.
  - different_from: brief labels for prior or sibling approaches this direction is structurally different from; use an empty list only when there is no meaningful comparator.
