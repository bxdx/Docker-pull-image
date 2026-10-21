@echo off
rem ============================================================
rem  dip - Docker Image Puller  (Web UI launcher)
rem  Double-click this file to open the graphical interface.
rem ============================================================
cd /d "%~dp0"

where python >nul 2>nul
if %errorlevel%==0 goto :uspython

where py >nul 2>nul
if %errorlevel%==0 goto :uspy

echo.
echo  [ERROR] Python 3 was not found in PATH.
echo  Please install Python 3.8+ and make sure "python" works in a terminal.
echo  Download: https://www.python.org/downloads/
echo.
pause
exit /b 1

:uspython
python -u dip.py web %*
goto :done

:uspy
py -3 -u dip.py web %*
goto :done

:done
echo.
echo  Server stopped.
pause
