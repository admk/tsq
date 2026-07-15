Resolve the in-progress Git integration in the current taskq-owned worktree.

Destination:
$destination

Queued change:
$change

Git operation and diagnostics:
$artifacts

Rules:
- Preserve the destination behavior first, then preserve as much of the queued change as remains compatible.
- Inspect Git status and resolve only the current conflicts. Do not introduce unrelated changes.
- Complete the current cherry-pick or rebase only when the combined result is coherent.
- If the changes cannot be integrated safely, abort the Git operation and explain why.
- Do not switch branches, edit the user's destination worktree, weaken tests, or ask the user questions.
