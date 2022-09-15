@REM if necessary, clone repo with:
@REM git clone https://github.com/AllenInstitute/np_data_validation.git

@REM this just allows us to run as admin
cd /d "%~dp0"

ECHO off
title Checking for valid backups and clearing local directories

git checkout main
git pull origin main

CALL conda env create --file environment.yml
CALL pip install -r requirements.txt

SET rig=%AIBS_COMP_ID%

IF %rig%==NP.1-Acq CALL C:\ProgramData\Miniconda3\Scripts\activate.bat C:\ProgramData\Miniconda3

CALL conda activate dv

CALL python data_validation.py
cmd \k