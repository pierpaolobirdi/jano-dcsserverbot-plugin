@echo off
setlocal EnableDelayedExpansion

:: Get the directory where this script is located (works from anywhere)
set "SCRIPT_DIR=%~dp0"

echo.
echo ================================================
echo  Jano Plugin Installer for DCSServerBot v3.2
echo ================================================
echo.

:: ── Detect DCSServerBot installation ─────────────────────────────────────────
set "DCSSB_PATH="

for %%P in (
    "C:\DCSServerBot"
    "D:\DCSServerBot"
    "E:\DCSServerBot"
    "L:\DCSServerBot"
    "%USERPROFILE%\DCSServerBot"
    "%USERPROFILE%\Documents\DCSServerBot"
) do (
    if exist "%%~P\config\main.yaml" (
        if "!DCSSB_PATH!"=="" set "DCSSB_PATH=%%~P"
    )
)

if not "!DCSSB_PATH!"=="" (
    echo Detected DCSServerBot at: !DCSSB_PATH!
    set /p CONFIRM="Is this correct? (Y/N): "
    if /i "!CONFIRM!"=="N" set "DCSSB_PATH="
)

if "!DCSSB_PATH!"=="" (
    set /p DCSSB_PATH="Enter the full path to your DCSServerBot installation: "
)

if not exist "!DCSSB_PATH!\config\main.yaml" (
    echo.
    echo ERROR: DCSServerBot not found at: !DCSSB_PATH!
    echo        Could not find config\main.yaml
    pause
    exit /b 1
)

echo.
echo Installing Jano to: !DCSSB_PATH!
echo.

:: ── Install pytz ──────────────────────────────────────────────────────────────
echo [1/4] Installing pytz...
if exist "%USERPROFILE%\.dcssb\Scripts\pip.exe" (
    "%USERPROFILE%\.dcssb\Scripts\pip.exe" install pytz --quiet
    if !ERRORLEVEL! == 0 (
        echo       OK - pytz installed successfully.
    ) else (
        echo       WARNING - Could not install pytz automatically.
        echo       Please run manually:
        echo       %%USERPROFILE%%\.dcssb\Scripts\pip install pytz
    )
) else (
    echo       WARNING - DCSServerBot Python environment not found at default location.
    echo       Please install pytz manually:
    echo       %%USERPROFILE%%\.dcssb\Scripts\pip install pytz
)

:: ── Copy plugin files ─────────────────────────────────────────────────────────
echo [2/4] Copying plugin files...
if not exist "!DCSSB_PATH!\plugins\jano" mkdir "!DCSSB_PATH!\plugins\jano"
if not exist "!DCSSB_PATH!\plugins\jano\db" mkdir "!DCSSB_PATH!\plugins\jano\db"

copy /Y "%SCRIPT_DIR%plugins\jano\commands.py"    "!DCSSB_PATH!\plugins\jano\commands.py"    > nul
copy /Y "%SCRIPT_DIR%plugins\jano\__init__.py"    "!DCSSB_PATH!\plugins\jano\__init__.py"    > nul
copy /Y "%SCRIPT_DIR%plugins\jano\listener.py"    "!DCSSB_PATH!\plugins\jano\listener.py"    > nul
copy /Y "%SCRIPT_DIR%plugins\jano\version.py"     "!DCSSB_PATH!\plugins\jano\version.py"     > nul
copy /Y "%SCRIPT_DIR%plugins\jano\db\tables.sql"  "!DCSSB_PATH!\plugins\jano\db\tables.sql"  > nul
echo       OK - Plugin files copied.

:: ── Copy config file (only if it doesn't exist) ───────────────────────────────
echo [3/4] Copying configuration file...
if not exist "!DCSSB_PATH!\config\plugins\jano.yaml" (
    if not exist "!DCSSB_PATH!\config\plugins" mkdir "!DCSSB_PATH!\config\plugins"
    copy /Y "%SCRIPT_DIR%config\plugins\jano.yaml" "!DCSSB_PATH!\config\plugins\jano.yaml" > nul
    echo       OK - jano.yaml created. Edit it to configure your roles and timezone.
) else (
    echo       SKIPPED - jano.yaml already exists, not overwritten.
    echo       Your existing configuration has been preserved.
)

:: ── Check main.yaml for jano entry ───────────────────────────────────────────
echo [4/4] Checking main.yaml...
findstr /C:"- jano" "!DCSSB_PATH!\config\main.yaml" > nul 2>&1
if !ERRORLEVEL! == 0 (
    echo       OK - jano already listed in main.yaml.
) else (
    echo       ACTION REQUIRED - Add the following to your config\main.yaml:
    echo.
    echo           opt_plugins:
    echo             - jano
    echo.
)

:: ── Done ─────────────────────────────────────────────────────────────────────
echo.
echo ================================================
echo  Installation complete!
echo ================================================
echo.
echo Next steps:
echo   1. Make sure 'jano' is listed under opt_plugins in config\main.yaml
echo   2. Edit config\plugins\jano.yaml to set your roles and timezone
echo   3. Restart DCSServerBot
echo   4. Use /jano setup to create your first instance
echo.
pause
