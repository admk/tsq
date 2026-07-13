# tq

`tq` is a small task queue CLI
for running shell commands in the background,
checking their status, reading their output,
and attaching to running tasks
when the backend supports it.

The default backend is `tmux`.
A `ts` backend is also available
for systems that use Task Spooler.

## Installation

Install the published package
as a standalone tool with `uv`:

```sh
uv tool install tsq
```

Install the latest version from GitHub:

```sh
uv tool install git+https://github.com/admk/tsq.git
```

Upgrade an existing install:

```sh
uv tool upgrade tsq
```

The installed command is `tq`.

Backend requirements:

| Backend | Required command | Notes |
| --- | --- | --- |
| `tmux` | `tmux` | Default backend. Queues jobs through a taskq broker tmux session and lets you attach to running jobs with `tq interact` / `tq i`. |
| `ts` | `ts` | Uses Task Spooler. Good for existing `ts` users, but interactive attach is not supported by this backend. |

## Quick Start

```sh
# Queue a command.
tq add python train.py

# Queue a command that needs 2 GPUs and 4 slots.
tq add -G 2 -N 4 python train.py --epochs 10

# Queue a command after jobs 1 and 2 finish successfully.
tq add -D 1,2 python evaluate.py

# List jobs. Running `tq` with no action is the same as `tq list`.
tq
tq list

# Show detailed metadata, including exit_code.
tq info 1

# Show output.
tq outputs 1
tq outputs -t 50 1

# Attach to a running tmux-backed job.
tq interact 1
tq i 1

# Wait for queued/running jobs to finish.
tq wait
```

Commands are stored as argv-style commands.
If you need shell syntax
such as redirection, pipes, `&&`, or compound commands,
make the shell explicit:

```sh
tq add sh -c 'echo hello > out.txt'
tq add sh -c 'make build && make test'
```

With the tmux backend,
environment variables visible to `tq add`
are captured for the queued job,
so one-off variables can be passed in the usual shell form:

```sh
FOO=bar tq add printenv FOO
```

With the tmux backend,
jobs can be queued from a specific git branch, tag, or commit:

```sh
tq add --ref main pytest
tq add --ref HEAD~1 sh -c 'make build && make test'
```

The ref is resolved to an exact commit when the job is queued.
The job runs in a detached git worktree
stored under taskq's job state directory.
`rerun` and `requeue` reuse the same resolved commit
and the original captured environment,
even if the branch has moved since the original job was added.
Worktrees remain inspectable until the job is removed
or the backend is reset.
Merging changes back is manual.

## Autonomous Exploration

`tq explore` runs parallel, agent-driven optimization campaigns on the
`tmux` backend. Start from a clean, attached local branch:

```sh
tq explore start "reduce broker latency" \
  --check 'pytest -q' \
  --score 'python benchmarks/broker.py' \
  --score-direction min

tq explore
tq explore status
tq explore inspect
tq explore pause
tq explore resume
tq explore stop
```

Configuration is divided by execution phase:

| Phase | Responsibility |
| --- | --- |
| `planning` | Generate distinct directions without editing code. |
| `optimization` | Make CPU-only implementation-code changes in worktrees. |
| `inspection` | Independently review completed attempts. |
| `validation` | Run trusted checks and comparative scoring. |
| `merge` | Rebase, review, and serialize accepted attempts. |
| `controller` | Schedule work, enforce the campaign deadline, and supervise heartbeats. |

Agent instructions are TOML templates rather than Python constants. Configure
`planning.prompt`, `optimization.prompt`, `optimization.adjust_prompt`,
`inspection.prompt`, `merge.review_prompt`, and `merge.rebase_prompt`.
Templates use `$name` placeholders such as `$objective`, `$direction`,
`$memory`, `$artifacts`, `$context`, `$direction_count`, and `$change_scope`;
write `$$` for a literal dollar sign. The shared
`explore.response_repair_prompt` controls structured-response repair turns.

Each attempt runs the configured agent in its own branch-backed worktree.
Actual concurrency is capped by both `explore.optimization.parallel` and the selected
queue's available slots. Planner batches prioritize independent directions
that can execute concurrently with minimal overlap or ordering dependencies.
Optimizer jobs receive no GPUs and leave checks and benchmarks to the
validation phase. Set `explore.validation.gpus` when those commands require
GPU allocation.
When an attempt finishes, a fresh reviewer autonomously inspects its status,
diff, and trusted checks or scores, then accepts it, requests an adjustment,
abandons it, or stops the campaign. Checks and scoring are optional, but
when supplied they are hard acceptance gates and cannot be waived by agents.
A failed candidate validation can be sent back for another optimization when
the reviewer chooses `adjust`, up to `optimization.max_adjustments`; baseline
validation failure instead fails the campaign.

Accepted attempts enter a FIFO merge queue. They are rebased, reviewed, and
merged one at a time into a campaign mainline; conflicts are returned to an
agent in the attempt worktree. Conflict resolution preserves current mainline
behavior first and the accepted optimization second. If they are incompatible,
the attempt branch is abandoned and the reason is recorded in campaign memory.
While this queue is non-empty, existing work may finish, be reviewed, and
receive adjustments, but no new direction starts. Once it drains,
planning resumes from the updated mainline. The final mainline is
fast-forwarded automatically when safe. If the target has diverged, taskq
prints the manual merge command and waits for the user to resolve conflicts.

Campaign state and evidence are durable. Confirmed results become persistent
project memory so later directions can avoid failed approaches and build on
successful ones. `pause`, controller restarts, and process failures preserve
this state.

Configured agent and validation commands still run with the invoking user's
OS privileges. Taskq isolates their Git checkouts and validates the resulting
commits, but arbitrary runners should provide their own filesystem sandbox.

## Configuration

`tq` reads TOML configuration in this order:

1. Built-in defaults.
2. `$XDG_CONFIG_HOME/tq/config.toml`, or `~/.config/tq/config.toml` when `XDG_CONFIG_HOME` is unset.
3. The nearest `.tq/config.toml` in the current directory or a parent.

Later files override earlier settings.
Use `-rc /path/to/tq.toml`
to read a single explicit config file instead.

Example config:

```toml
backend = "tmux"
queue = "default"
slots = "auto"
socket = "taskq"

[alloc]
gpus = 0
slots = 1

[explore]
command = ["codex", "exec", "{}"]
timeout = 1800

[explore.planning]

[explore.optimization]
parallel = 4
# Set any max_* value to 0 to disable that cap.
max_adjustments = 3
max_files = 0
max_lines = 300
protected = [".tq/**", "test/**", "tests/**", "benchmark/**", "benchmarks/**"]

[explore.inspection]

[explore.validation]
gpus = 0
checks = []
min_improvement = 0.0

[explore.merge]
max_accepted_attempts = 0

[explore.controller]
max_wall_time = 28800
interval = 5
heartbeat_timeout = 30

[env]
OMP_NUM_THREADS = "1"

[backends.tmux]
command = "tmux"
history_limit = 100000
broker_interval = 1
gpu_free_perc = 90

[backends.ts]
command = "ts"

[queues.gpu]
slots = 4

[queues.gpu.alloc]
gpus = 1
slots = 1
```

Useful config keys:

| Key | Meaning |
| --- | --- |
| `backend` | Default backend: `tmux`, `ts`, or `dummy`. |
| `queue` | Default queue. Queues isolate state and can override settings. |
| `slots` | Queue capacity. `"auto"` uses GPU count when `nvidia-smi` is available, otherwise `1`. |
| `socket` | Shared backend socket name. For tmux this selects the socket file under the taskq cache directory. |
| `[alloc].gpus` | Default GPUs required per new job. |
| `[alloc].slots` | Default slots required per new job. |
| `[explore.<phase>]` | Phase-specific agent commands, timeouts, validation, safety limits, and controller timing for autonomous campaigns. |
| `[env]` | Environment variables exported into jobs. |
| `[backends.tmux].gpu_free_perc` | GPU memory-free threshold used by the tmux broker when allocating GPUs. |
| `[queues.<name>]` | Per-queue overrides. Use `tq -Q <name> ...` to select a queue. |

Inspect or edit configuration with:

```sh
tq config
tq config slots
tq config slots 4
tq config alloc.gpus 1
tq config alloc.gpus null
```

## Backends

### tmux

The tmux backend stores
queued job metadata under `$XDG_CACHE_HOME/tq`,
or `~/.cache/tq` when `XDG_CACHE_HOME` is unset,
starts a broker session if needed,
and uses a shared tmux socket
for broker and job sessions.
Jobs are started in their own tmux sessions.
You can attach to a running job:

```sh
tq i 12
```

The taskq tmux server starts with packaged taskq tmux defaults.
It does not load your local `~/.tmux.conf`
and sources tmux-specific customization
from `$XDG_CONFIG_HOME/tq/tmux.conf`
or the nearest `.tq/tmux.conf`
in the current directory or a parent.

GPU jobs are queued until enough GPUs appear free
according to `nvidia-smi` and `gpu_free_perc`.
CPU jobs run with `TASKQ_GPU_IDS=-1`.

### ts

The `ts` backend delegates queueing to Task Spooler.
It maps Task Spooler states to `tq` statuses
and supports output, kill, remove, rerun, and requeue.
It does not support `tq interact`.

## Cheatsheet

### Global Arguments

| Command | Purpose |
| --- | --- |
| `tq` | List jobs using the default action. |
| `tq -V` / `tq --version` | Print version and exit. |
| `tq -rc FILE ...` | Use one explicit config file. |
| `tq -b tmux ...` | Use a backend for this command. |
| `tq -Q QUEUE ...` | Use a queue for this command. |
| `tq ACTION -h` | Show help for an action. |

### Job Selection And Filters

Most read/write actions
accept an optional job selector and status filters.

| Argument | Purpose | Example |
| --- | --- | --- |
| `ID` | Select one job. | `tq info 7` |
| `A-B` | Select a range. | `tq remove 3-8` |
| `A-B,C,D-E` | Select multiple ranges/IDs. | `tq info 1-3,8,10-12` |
| `-` | Read IDs from stdin. | `tq ids --failed \| head -n 10 \| tq remove -` |
| `-A`, `--all` | Select all jobs explicitly. Useful for dangerous actions. | `tq remove --all` |
| `-r`, `--running` | Filter running jobs. | `tq list --running` |
| `-q`, `--queued` | Filter queued jobs. | `tq list --queued` |
| `-s`, `--success` | Filter successful jobs. | `tq remove --success` |
| `-f`, `--failed` | Filter failed jobs. | `tq info --failed` |
| `-k`, `--killed` | Filter killed jobs. | `tq list --killed` |
| `-i`, `--interrupted` | Filter jobs whose tmux session disappeared without a recorded exit. | `tq info -i` |

Write actions require IDs or filters
to avoid accidental bulk changes.

### Add Jobs

| Command | Purpose |
| --- | --- |
| `tq add CMD...` / `tq a CMD...` | Queue one command. |
| `tq add -G N CMD...` | Require `N` GPUs. |
| `tq add -N N CMD...` | Require `N` slots. |
| `tq add -D IDS CMD...` | Only start after the selected jobs complete successfully. Accepts IDs and ranges like `1-3,5`. |
| `tq add -R N CMD...` | Queue each command `N` times, chaining repeated instances of the same command. |
| `tq add -R N --no-chain CMD...` | Queue repeated instances without adding dependencies between them. |
| `tq add -u CMD...` | Queue only if the command is not already queued/known. |
| `tq add -i CMD...` | Queue one command and attach when it starts. |
| `tq add -f FILE` | Read one command per line from a file. |
| `tq add -f -` | Read commands from stdin. |
| `tq add -d CMD...` | Dry run; print resolved `tq add` command lines without queueing. |
| `tq add -s SEP CMD...` | Use `SEP` (defaults to `,`) when expanding stdin fields into `@1`, `@2`, etc. |

Expansion helpers:

| Syntax | Meaning | Example |
| --- | --- | --- |
| `{a,b}` | Set expansion. | `tq add 'echo {train,test}'` |
| `[1-3]` | Numeric range expansion. | `tq add 'echo seed=[1-3]'` |
| `@u` | Stable unique identifier. | `tq add 'run --name @u'` |
| `@1`, `@2` | Fields from stdin lines. | `printf 'a,1\nb,2\n' \| tq add 'echo @1 @2'` |

### Read Jobs

| Command | Purpose |
| --- | --- |
| `tq list` / `tq ls` | Show compact job table. |
| `tq list -n 2` | Refresh the table every 2 seconds. |
| `tq list -c id,status,slots,gpus,command` | Choose table columns. |
| `tq list -c +output` | Add a column to the default table. |
| `tq list -c -time` | Remove a column from the default table. |
| `tq list -l N` | Truncate long command/output columns to `N` chars. |
| `tq list -t FORMAT` | Set the `tabulate` table format. |
| `tq ids` / `tq id` | Print matching job IDs. |
| `tq info ID` | Show full metadata for a job. |
| `tq commands` / `tq cmd` / `tq c` | Show job commands with IDs. |
| `tq commands -j` | Show commands without job IDs. |
| `tq outputs ID` / `tq o ID` | Show output for jobs. |
| `tq outputs -t N ID` | Show the last `N` output lines. |
| `tq outputs -R ID` | Print raw output without `>` formatting. |
| `tq outputs -I ID` | Attach/follow interactively when supported. |
| `tq interact ID` / `tq i ID` | Attach to a running job when supported. |
| `tq wait` / `tq w` | Wait until selected running/queued jobs finish. |
| `tq wait -p` | Show a progress bar while waiting. |
| `tq export` / `tq e` | Export job data as JSON. |
| `tq export -e yaml` | Export as YAML. |
| `tq export -e toml` | Export as TOML. |
| `tq export -t N` | Include output tail in exports. |

Available `list` columns
are `id`, `status`, `slots`, `gpus`, `gpu_ids`, `enqueue`,
`start`, `end`, `time`, `command`, and `output`.

### Write Jobs

| Command | Purpose |
| --- | --- |
| `tq kill ID` / `tq k ID` | Kill running jobs. |
| `tq remove ID` / `tq rm ID` | Remove jobs and metadata. |
| `tq rerun ID` / `tq rr ID` | Add a new job with the same command/resources. |
| `tq rerun -R N ID` | Rerun each selected job `N` times, chaining repeated instances of the same command. |
| `tq rerun -R N --no-chain ID` | Rerun repeated instances without adding dependencies between them. |
| `tq requeue ID` / `tq rq ID` | Rerun then remove the old job. |
| `tq requeue -R N ID` | Requeue each selected job `N` times, chaining repeated instances of the same command. |
| `tq requeue -R N --no-chain ID` | Requeue repeated instances without adding dependencies between them. |
| `tq remove --success` | Remove successful jobs. |
| `tq kill --running` | Kill all running jobs. |
| `tq remove --all` | Remove all jobs. |
| `tq remove -d ID` | Dry run; print what would be removed. |

### Backend And Config

| Command | Purpose |
| --- | --- |
| `tq backend info` / `tq b info` | Show active backend details. |
| `tq backend command ARGS...` | Run a backend-specific command. For tmux this runs `tmux` with taskq socket/config args. |
| `tq backend reset` | Remove all known jobs and kill backend sessions/processes. |
| `tq config` | Print resolved config. |
| `tq config KEY` | Print one config key. |
| `tq config KEY VALUE` | Set a config key in the active rc file. |
| `tq config KEY null` | Delete a config key from the active rc file. |

### Explore

| Command | Purpose |
| --- | --- |
| `tq explore start OBJECTIVE` | Start a tmux-only campaign from the clean current branch. |
| `tq explore` | List active and recent campaigns. |
| `tq explore status [CAMPAIGN]` | Show campaign progress, attempts, and merge queue. |
| `tq explore inspect [CAMPAIGN] [ATTEMPT]` | Show an attempt's decisions, evidence, and diff. |
| `tq explore pause [CAMPAIGN]` | Stop scheduling new campaign work. |
| `tq explore resume [CAMPAIGN]` | Resume durable campaign reconciliation and scheduling. |
| `tq explore stop [CAMPAIGN]` | Drain accepted work and land the campaign mainline. |

## Statuses

| Status | Meaning |
| --- | --- |
| `queued` | Waiting for slots or GPU allocation. |
| `running` | Started and currently running. |
| `success` | Finished with exit code `0`. |
| `failed` | Finished with a non-zero exit code or failed to start. |
| `killed` | Killed through `tq kill` or backend equivalent. |
| `interrupted` | A tmux job session disappeared without a recorded exit code. |
