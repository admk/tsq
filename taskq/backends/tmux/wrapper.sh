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
command_result_file={command_result_file}
merge_enabled={merge_enabled}
submission_id={submission_id}
printf "[taskq] job {job_id} started at %s\n" "$(date)"
if [ "$#" -eq 0 ]; then
    exitcode=127
else
    "$@"
    exitcode=$?
fi
command_end_time="$(date '+%Y-%m-%dT%H:%M:%S')"
if [ "$merge_enabled" -eq 1 ] && [ "$exitcode" -eq 0 ]; then
    printf "[taskq] job {job_id} command completed; waiting for merge at %s\n" "$(date)" >> "$output_file"
else
    printf "[taskq] job {job_id} finished with exit code %s at %s\n" "$exitcode" "$(date)" >> "$output_file"
fi
command_result_tmp="${{command_result_file}}.tmp.$$"
printf '{{"exitcode":%s,"end_time":"%s","submission_id":"%s"}}\n' "$exitcode" "$command_end_time" "$submission_id" > "$command_result_tmp"
mv -f "$command_result_tmp" "$command_result_file"
if [ "$merge_enabled" -eq 1 ] && [ "$exitcode" -eq 0 ]; then
    exit 0
fi
exit "$exitcode"
