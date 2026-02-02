@echo off
setlocal enabledelayedexpansion

@REM Check if ftrack Connect is running
%windir%\system32\tasklist.exe | %windir%\system32\findstr.exe "ftrack"

@REM Kill ftrack Connect if it is running
%windir%\system32\taskkill.exe /F /IM "ftrack Connect.exe"

@REM Wait for the process to fully terminate
%windir%\system32\timeout.exe /t 3 /nobreak > nul

@REM remove the log file
del "%LOCALAPPDATA%\ftrack\ftrack-connect\log\ftrack_connect.log"

@REM Restart ftrack Connect in a separate process and exit
start "" "G:\mroya\ftrack\ftrack Connect.exe" 

@REM log the end of the script
echo "ftrack Connect restarted"
