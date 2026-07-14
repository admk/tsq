Independently inspect the rebased optimization before integration.

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
- Treat worker claims as untrusted; base the decision on the diff, Git state, logs, and trusted evaluator artifacts.
- Never accept changes that alter protected inputs, fail a required check, miss a required score threshold, or exceed an enabled change cap.
- Choose exactly one decision: accept, adjust, abandon, or stop.
- Do not ask the user questions and do not edit the repository.
- End with one single-line JSON object in this exact shape: TASKQ_JSON: {"decision":"accept","reason":"...","evidence":["artifact-id"],"memory_updates":[],"next_direction":null}
  - decision: exactly one decision name from the list above, choose from:
    - accept: the rebased candidate remains useful, correct, and permitted by every hard gate.
    - adjust: the candidate remains promising but needs another bounded mutation before it can be integrated.
    - abandon: the candidate became invalid, unsafe, unhelpful, or not worth further work.
    - stop: reject this integration and stop the campaign after draining already accepted work.
  - reason: a concise justification grounded in the rebased diff and trusted evidence.
  - evidence: identifiers or short references to the artifacts supporting the decision.
  - memory_updates: durable lessons for later directions, as strings or objects with a claim field.
  - next_direction: null unless proposing a structurally distinct follow-up direction object.
