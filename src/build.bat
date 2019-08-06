@echo off

if not exist "C:\Program Files (x86)\Microsoft Visual Studio\2017\Enterprise\Common7\Tools\VsDevCmd.bat" goto novisualstudio
call "C:\Program Files (x86)\Microsoft Visual Studio\2017\Enterprise\Common7\Tools\VsDevCmd.bat"

CD /D "%~dp0"

msbuild SqlServerInfo\SqlServerInfo.csproj /p:Configuration=Debug /t:Rebuild
if errorlevel 1 goto berror

msbuild SqlServerInfo\SqlServerInfo.csproj /p:Configuration=Release /t:Rebuild
if errorlevel 1 goto berror

goto :EOF


:novisualstudio
echo We could not find the Visual Studio 2017 command line tools.
pause
goto :EOF

:berror
pause