@REM if necessary, clone repo with:
@REM git clone https://github.com/AllenInstitute/np_data_validation.git

@REM this just allows us to run as admin
cd /d "%~dp0"

ECHO off
title Checking status of files in session directories on rigs, np-exp, lims

git checkout main
git pull origin main

CALL conda env create --file environment.yml
CALL pip install -r requirements.txt

SET rig=%AIBS_COMP_ID%

IF %rig%==NP.1-Acq CALL C:\ProgramData\Miniconda3\Scripts\activate.bat C:\ProgramData\Miniconda3

CALL conda activate dv

CALL python session_dir_status.py -h

set /p input=Please input one of: path to a session directory, path to a platform json, just a session folder name (e.g. 123456789_366122_20220618) : 

CALL python session_dir_status.py %input%
cmd \k%input%