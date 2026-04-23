@echo off
setlocal

set "SCRIPT_DIR=%~dp0"
for %%I in ("%SCRIPT_DIR%..") do set "REPO_ROOT=%%~fI"
set "PYTHON_EXE=%REPO_ROOT%\.venv\Scripts\python.exe"

if not exist "%PYTHON_EXE%" (
  echo Repo venv Python was not found at "%PYTHON_EXE%". 1>&2
  echo Create the repo venv first, then retry. 1>&2
  exit /b 1
)

"%PYTHON_EXE%" %*
exit /b %ERRORLEVEL%
