@echo off
setlocal

set "SCRIPT_DIR=%~dp0"

start "Signal API" powershell -NoExit -Command "Set-Location '%SCRIPT_DIR%'; uvicorn signal_api:app --host 0.0.0.0 --port 8000"
start "ngrok" powershell -NoExit -Command "Set-Location '%SCRIPT_DIR%'; ngrok http 8000"
