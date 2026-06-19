#!/bin/sh
set +e
set -- {argv}
mkdir -p {output_dir}
touch {output_file}
start_file={start_file}
i=0
while [ "$i" -lt 100 ]; do
    [ -e "$start_file" ] && break
    sleep 0.05
    i=$((i + 1))
done
rm -f "$start_file"
{env_exports}
if [ "${{TASKQ_GPU_IDS+x}}" ]; then
    export CUDA_VISIBLE_DEVICES="$TASKQ_GPU_IDS"
fi
export TASKQ_JOB_ID={job_id}
export TASKQ_SESSION={session}
output_file={output_file}
printf "[taskq] job {job_id} started at %s\n" "$(date)"
if [ "$#" -eq 0 ]; then
    exitcode=127
else
    "$@"
    exitcode=$?
fi
printf "[taskq] job {job_id} finished with exit code %s at %s\n" "$exitcode" "$(date)" >> "$output_file"
exit "$exitcode"
