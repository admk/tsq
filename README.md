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
| `tq requeue ID` / `tq rq ID` | Rerun then remove the old job. |
| `tq requeue -R N ID` | Requeue each selected job `N` times, chaining repeated instances of the same command. |
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

## Statuses

| Status | Meaning |
| --- | --- |
| `queued` | Waiting for slots or GPU allocation. |
| `running` | Started and currently running. |
| `success` | Finished with exit code `0`. |
| `failed` | Finished with a non-zero exit code or failed to start. |
| `killed` | Killed through `tq kill` or backend equivalent. |
| `interrupted` | A tmux job session disappeared without a recorded exit code. |
