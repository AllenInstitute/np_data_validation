ECHO off
CALL conda activate loom
python "check_disks.py"
cmd \k