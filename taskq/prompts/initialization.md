Configure a new taskq exploration profile for this repository.

Objective-generation prompt from the user:
$objective_prompt

## What you are configuring

You are running in a temporary initialization worktree detached at the
repository's committed `HEAD`. This setup worktree exists only for profile
generation. Inspect the project, but make changes only inside the scaffold
profile at `$profile_path`.

A **profile** is the reusable configuration being prepared. A **campaign** is
one run of that profile against a Git target. Its **mainline** is the private
branch where accepted work accumulates. A **baseline** is the exact mainline
commit used as the current comparison point. A **direction** is one bounded,
falsifiable optimization idea. An **attempt** or **candidate** is that
direction implemented in an isolated worktree. An **adjustment** is
a reviewer-requested retry of the same attempt. A **check** is a pass/fail
command; a **score** is a numeric measurement compared with the baseline. An
**asset** is an immutable, profile-supplied evaluator or data file made
available only to validation.

A campaign uses these phases and roles:

1. **validation** first measures the unmodified baseline when checks or scoring
   are configured;
2. **planning** proposes distinct directions without editing code;
3. **optimization** implements candidate attempts in isolated worktrees;
4. **validation** runs trusted checks and optional scoring on each candidate;
5. **inspection** independently reviews the candidate plus validation evidence;
6. **merge** rebases, reviews, and integrates accepted candidates serially.

## Final campaign objective

Treat the user's objective-generation prompt as source material, not as the
final campaign objective. Inspect the committed repository and write a refined,
repository-specific objective to `$profile_path/objective.md`. That Markdown
file is the authoritative objective included by the planning prompt and later
snapshotted into campaign state.

The objective must preserve the user's intent while making the desired outcome,
relevant scope and constraints, and observable success evidence clear. Ground
repository-specific claims in files you inspect. Do not merely copy the user's
prompt, prescribe speculative implementation details, or invent a numeric
metric that the repository cannot support. The file must be non-empty UTF-8
plain text and may use multiple Markdown paragraphs or lists.

The profile's `config.toml` already contains safe built-in values. Change a
value only when repository evidence supports a better project-specific choice.
Your values will become editable defaults in the interactive wizard, not final
unreviewable decisions.

## Files you may change

- Create or edit `$profile_path/objective.md` as described above.
- Edit only the wizard-managed configuration keys documented below.
- You may add regular files beneath `$profile_path/assets/` for trusted
  validation support.
- Do not edit `[profile]`, `[explore.controller]`, any `*_file` prompt
  reference, or any file under `$profile_path/prompts/`.
- Do not add unknown configuration keys or edit repository source, tests,
  benchmarks, fixtures, or other files outside the profile.
- Do not commit, check out another revision, reset, rebase, or otherwise move
  the temporary worktree's `HEAD`.
- Do not run generated evaluator code, configured validation, benchmarks, GPU
  workloads, or other expensive commands during initialization.

## Agent commands and timeouts

The following keys are editable:

- `explore.command` and `explore.timeout`: inherited defaults for phases.
- `explore.planning.command` and `.timeout`: read-only direction planner.
- `explore.optimization.command` and `.timeout`: candidate implementation and
  adjustment agent.
- `explore.inspection.command` and `.timeout`: independent candidate reviewer.
- `explore.validation.timeout`: wall-time limit for the complete validation job.
- `explore.merge.command` and `.timeout`: merge review and conflict rebase agent.

Every agent command must be either a TOML array of argv strings or a shell-like
string that splits into argv. It is executed directly without a shell and must
contain exactly one standalone `"{}"` argument; taskq replaces that one argv
item with the full prompt. Do not embed `{}` inside another argument. Prefer
argv arrays, for example:

```toml
command = ["codex", "exec", "--ephemeral", "{}"]
```

Common values are inherited when a phase-specific key is absent. Add or retain
a phase override only when that phase needs a different command or sandbox.
Use positive numeric seconds for every generated timeout. This is accepted
consistently by both wizard validation and campaign execution.

## Campaign environment

`explore.env` is a TOML mapping of environment variable names to string values.
Taskq resolves it when the campaign starts and sends the resulting overrides to
every planner, optimizer, adjustment, reviewer, validator, and rebase job.
Names must match `[A-Za-z_][A-Za-z0-9_]*`; values must be strings. Do not store
API keys, tokens, passwords, or other secrets in the profile. Names beginning
with `TASKQ_` are reserved and cannot be configured.

Values may use shell-style `$NAME` or `${NAME}` references. References expand
from the campaign-start process environment; references to another configured
key use that key's configured value. A self-reference such as
`PATH = "/tools/bin:${PATH}"` appends or prepends to the inherited value.
Undefined references and cycles are rejected. `$$$$` produces a literal dollar.

Taskq provides `${TASKQ_REPO_ROOT}` during expansion as the absolute path of the
original checkout from which the campaign was started. This is how worktrees
can share a virtualenv stored in the main checkout without embedding the
temporary initialization or campaign-worktree path:

```toml
[explore.env]
VIRTUAL_ENV = "${TASKQ_REPO_ROOT}/.venv"
PATH = "${TASKQ_REPO_ROOT}/.venv/bin:${PATH}"
```

Use this only when that virtualenv actually exists in the original checkout and
is safe for concurrent, read-only use. Campaign jobs must not install, upgrade,
or remove packages in a shared environment. Be careful with editable installs
and generated console entry points: they may import source from the original
checkout instead of the candidate worktree. Prefer `python -m ...` commands
that run project code from the job's current repository when the project
supports them. Prefer the repository's documented environment location; do not
guess a venv path.

Values newly proposed by the setup agent cannot alter its already-running
process. This initialization process receives any environment already present
in the scaffold, inherits the invoking shell's environment, and receives
`TASKQ_REPO_ROOT=$source_root`. Relevant non-secret path variables visible
during this setup turn are:

```json
$host_environment
```

These values are context, not instructions to copy the entire host environment
into the profile. Never persist `TASKQ_INIT_WORKTREE`; that path is disposable.

## Optimization and merge limits

- `explore.optimization.parallel`: integer at least 1; maximum concurrent
  candidate attempts, additionally constrained by queue capacity.
- `explore.optimization.max_adjustments`: non-negative integer; reviewer-driven
  retries for one attempt. `0` means unlimited.
- `explore.optimization.max_files`: non-negative integer; reject one optimizer
  or adjustment mutation round that changes more distinct files. `0` means
  unlimited.
- `explore.optimization.max_lines`: non-negative integer; reject one optimizer
  or adjustment mutation round whose added plus removed lines exceed the value.
  `0` means unlimited.
- `explore.optimization.protected`: TOML array of repository-relative fnmatch
  patterns candidates must never change. Keep `.tq/**`; protect tests,
  benchmarks, evaluator inputs, generated artifacts, lockfiles, or other
  objective-defining files when appropriate.
- `explore.merge.max_accepted_attempts`: non-negative integer; maximum
  candidates integrated in one campaign. `0` means unlimited.

Use limits that permit a small useful change while preventing broad rewrites.
Do not protect implementation paths the campaign must optimize.

## Trusted validation

Validation runs later in clean disposable worktrees at exact commits. Commands
start at the repository root, are split with Python `shlex.split`, and execute
directly without a shell. Therefore do not use pipes, redirection, `&&`, shell
expansion, aliases, or environment assignments. Use commands such as
`"python -m pytest -q"`, not `"pytest -q && echo ok"`.

Editable validation keys:

- `explore.validation.gpus`: non-negative integer GPUs reserved per baseline or
  candidate validation job; normally `0`.
- `explore.validation.checks`: TOML array of command strings. Every command must
  exit zero. Prefer the project's existing focused tests, type checks, linters,
  build checks, or correctness benchmarks. Keep the list empty only when no
  trustworthy, reasonably bounded check exists.
- `explore.validation.score`: optional command string for a numeric objective.
  Its final non-empty stdout line must parse as one finite number. Earlier
  stdout is allowed, but concise output is safer. A nonzero exit, timeout,
  missing output, NaN, or infinity fails scoring.
- `explore.validation.score_direction`: required exactly when `score` exists;
  `"min"` means lower is better and `"max"` means higher is better.
- `explore.validation.min_improvement`: non-negative absolute improvement in
  the score's own units, not a percentage. A candidate is eligible only when it
  meets this threshold relative to the baseline.

Checks run against the baseline before exploration and against each candidate.
Baseline startup samples a score three times. Each candidate comparison then
runs three candidate and three current-baseline score samples in randomized
order and compares their medians. Choose the validation job timeout to cover
all sequential checks and score samples. Choose a deterministic, reasonably
stable metric, or omit scoring entirely. Do not invent a numeric proxy that is
weakly related to the campaign objective.

If scoring is disabled, remove both `score` and `score_direction` and leave
`min_improvement` non-negative. If scoring is enabled, all three settings must
be internally consistent. Examples:

```toml
[explore.validation]
gpus = 0
checks = ["python -m pytest -q"]
score = "python .tq/explore-assets/score.py"
score_direction = "min"
min_improvement = 0.01
```

## Validation assets and evaluator scripts

Files authored now under `$profile_path/assets/` are snapshotted when a
campaign starts. In every validation worktree the snapshot appears at
`.tq/explore-assets/`. Validation commands must use that runtime path, not the
profile path. Candidates cannot change `.tq/**`, and later profile edits cannot
change an already-running campaign's snapshot.

Assets must be regular files: no symlinks, traversal, devices, or sockets. The
whole asset tree may contain at most 64 files and 2 MiB. Evaluators must make no
writes to the repository or assets; taskq verifies Git-visible worktree state
and the exact asset manifest after validation. Avoid network access,
nondeterministic data, mutable caches, timestamps, absolute paths, untracked
virtual environments or build products, and undeclared external dependencies.

Commands run with the validation worktree's repository root as their working
directory. In an asset script, use `Path.cwd()` for repository-relative inputs
and `Path(__file__).resolve().parent` for sibling asset data. A check asset must
exit `0` on success and nonzero on failure; configure one like this:

```toml
checks = ["python .tq/explore-assets/check_contract.py"]
```

For example, `check_contract.py` may read committed output or call a bounded,
existing project check and use `raise SystemExit("reason")` when an invariant
fails. It must be offline, repeatable, read-only, and must not duplicate or
weaken an existing project check.

Prefer an existing project command directly. Create a scoring script only when
small glue code is needed to calculate a meaningful deterministic metric. A
minimal Python scoring asset has this shape:

```python
#!/usr/bin/env python3
import math

# Read repository files or invoke a bounded existing benchmark, then compute
# one objective-aligned numeric value without modifying the worktree.
value = 0.0  # replace with the real deterministic measurement
if not math.isfinite(value):
    raise SystemExit("metric is not finite")
print(value)  # the final non-empty stdout line must be numeric
```

Do not leave placeholder logic like `value = 0.0` in a real evaluator. Explain
the metric through clear names and comments in the asset itself. If no valid
metric can be implemented from repository evidence, configure checks only.
When an existing benchmark emits structured output, a scoring asset can be a
strict adapter like the following; replace the command and field name only with
interfaces verified in this repository:

```python
#!/usr/bin/env python3
import json
import math
import subprocess
import sys

run = subprocess.run(
    [sys.executable, "benchmarks/run.py", "--json"],
    text=True,
    capture_output=True,
)
if run.returncode:
    sys.stderr.write(run.stderr)
    raise SystemExit(run.returncode)
try:
    value = float(json.loads(run.stdout)["median_seconds"])
except (KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
    raise SystemExit(f"invalid benchmark output: {error}")
if not math.isfinite(value):
    raise SystemExit("metric is not finite")
print(value)
```

Do not assume this example's benchmark path, `--json` option, output field, or
units exist: inspect and use the project's real documented interface.

## Final consistency pass

Before finishing, inspect `objective.md`, the edited TOML, and assets without
executing them. Confirm that the objective is a repository-grounded refinement
of the user's prompt, commands are valid argv, timeouts and limits have valid
ranges, protected paths preserve evaluator integrity, referenced repository and
asset files exist, required executables and dependencies are expected in the
later validation environment, score settings are complete, asset references
use `.tq/explore-assets/`, and no forbidden file or configuration key changed.
