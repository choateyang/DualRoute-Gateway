@echo off
setlocal
cd /d "%~dp0"
chcp 65001 >nul
set "PYTHONUTF8=1"

where py >nul 2>nul
if %errorlevel%==0 (
  py -3 tools\freebuff\get_token.py
  goto :done
)

where python >nul 2>nul
if %errorlevel%==0 (
  python tools\freebuff\get_token.py
  goto :done
)

echo Python 3 was not found. Install Python 3 and enable "Add Python to PATH".

:done
echo.
pause
