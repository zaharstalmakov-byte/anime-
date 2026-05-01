@echo off
REM ===================================================================
REM  ANIMEFLOW launcher (Windows 7+ / Python 3.8+).
REM  - installs dependencies from requirements.txt (idempotent, --user fallback)
REM  - starts uvicorn on http://127.0.0.1:8080
REM ===================================================================
setlocal
cd /d "%~dp0"

REM ---- pick a python launcher --------------------------------------
set "PY="
where py >nul 2>nul && set "PY=py -3"
if "%PY%"=="" (
    where python >nul 2>nul && set "PY=python"
)
if "%PY%"=="" (
    echo [ANIMEFLOW] Python not found. Please install Python 3.8 or newer
    echo            from https://www.python.org/downloads/  and re-run.
    pause
    exit /b 1
)

echo [ANIMEFLOW] Using interpreter: %PY%
%PY% --version

REM ---- install / refresh dependencies ------------------------------
echo [ANIMEFLOW] Installing requirements...
%PY% -m pip install --upgrade pip --quiet
%PY% -m pip install -r requirements.txt --quiet
if errorlevel 1 (
    echo [ANIMEFLOW] System-wide install failed, retrying with --user ...
    %PY% -m pip install -r requirements.txt --user --quiet
)

REM ---- ensure session secret ---------------------------------------
if "%SESSION_SECRET%"=="" set "SESSION_SECRET=local-dev-secret-change-me"

REM ---- launch ------------------------------------------------------
echo [ANIMEFLOW] Starting server at http://127.0.0.1:8080  (Ctrl+C to stop)
%PY% -m uvicorn main:app --host 127.0.0.1 --port 8080

pause
endlocal
