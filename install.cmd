@echo off
setlocal EnableDelayedExpansion

set "SCRIPT_DIR=%~dp0"

:: -- Version being installed: read from commands.py (single source of truth) ----
:: The line in commands.py must keep this exact format:  COMMANDS_VERSION = "x.y.z"
set "NEW_VER=unknown"
if exist "%SCRIPT_DIR%plugins\jano\commands.py" for /f "tokens=3" %%V in ('findstr /B /C:"COMMANDS_VERSION" "%SCRIPT_DIR%plugins\jano\commands.py" 2^>nul') do set "NEW_VER=%%~V"

:: -- Colors (ANSI escape codes, Windows 10/11 only; left empty on older systems) -
set "NEWC="
set "OLDC="
set "OKC="
set "ERRC="
set "ACTC="
set "RSTC="
set "OFF="
ver | findstr /C:" 10." > nul
if !ERRORLEVEL! == 0 (
    for /f %%E in ('echo prompt $E ^| cmd') do set "ESC=%%E"
    set "NEWC=!ESC![1;92m"
    set "OLDC=!ESC![31m"
    set "OKC=!ESC![94m"
    set "ERRC=!ESC![91m"
    set "ACTC=!ESC![96m"
    set "RSTC=!ESC![93m"
    set "OFF=!ESC![0m"
)

echo.
echo ============================================================
echo  Jano Plugin - Installer / Updater --^> Ver. !NEWC!!NEW_VER!!OFF!
echo ============================================================
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

set "SHOWN="

:: A detected path is shown together with the installed/new version BEFORE asking to confirm it
if not "!DCSSB_PATH!"=="" (
    echo Detected DCSServerBot at: !DCSSB_PATH!
    set "OLD_VER="
    set "OLD_FILE=!DCSSB_PATH!\plugins\jano\commands.py"
    if exist "!OLD_FILE!" (
        set "OLD_VER=unknown (no version in the installed file)"
        for /f "tokens=3" %%V in ('findstr /B /C:"COMMANDS_VERSION" "!OLD_FILE!" 2^>nul') do set "OLD_VER=%%~V"
    )
    echo Installing Jano !NEWC!Ver. !NEW_VER!!OFF! to: !DCSSB_PATH!
    if not defined OLD_VER (
        echo Installed now: none ^(new install^)
    ) else if "!OLD_VER!"=="!NEW_VER!" (
        echo Installed now: !NEWC!Ver. !NEW_VER!!OFF! ^(same version - files will be refreshed^)
    ) else (
        echo Installed now: !OLDC!Ver. !OLD_VER!!OFF! --^> updating to !NEWC!Ver. !NEW_VER!!OFF!
    )
    set "SHOWN=1"
    set /p CONFIRM="Is this correct? (Y/N): "
    if /i "!CONFIRM!"=="N" set "DCSSB_PATH="
    if /i "!CONFIRM!"=="N" set "SHOWN="
)

if "!DCSSB_PATH!"=="" (
    set /p DCSSB_PATH="Enter the full path to your DCSServerBot installation: "
)

if not exist "!DCSSB_PATH!\config\main.yaml" (
    echo.
    echo !ERRC!ERROR!OFF!: DCSServerBot not found at: !DCSSB_PATH!
    echo        Could not find config\main.yaml
    pause
    exit /b 1
)

:: A path typed by hand is shown with the versions right after it is validated
if not defined SHOWN (
    set "OLD_VER="
    set "OLD_FILE=!DCSSB_PATH!\plugins\jano\commands.py"
    if exist "!OLD_FILE!" (
        set "OLD_VER=unknown (no version in the installed file)"
        for /f "tokens=3" %%V in ('findstr /B /C:"COMMANDS_VERSION" "!OLD_FILE!" 2^>nul') do set "OLD_VER=%%~V"
    )
    echo Installing Jano !NEWC!Ver. !NEW_VER!!OFF! to: !DCSSB_PATH!
    if not defined OLD_VER (
        echo Installed now: none ^(new install^)
    ) else if "!OLD_VER!"=="!NEW_VER!" (
        echo Installed now: !NEWC!Ver. !NEW_VER!!OFF! ^(same version - files will be refreshed^)
    ) else (
        echo Installed now: !OLDC!Ver. !OLD_VER!!OFF! --^> updating to !NEWC!Ver. !NEW_VER!!OFF!
    )
)
echo.

:: ── Install tzdata ────────────────────────────────────────────────────────────
echo [1/4] Installing tzdata (Windows timezone data)...
if exist "%USERPROFILE%\.dcssb\Scripts\pip.exe" (
    "%USERPROFILE%\.dcssb\Scripts\pip.exe" install tzdata --quiet
    if !ERRORLEVEL! == 0 (
        echo       !OKC!OK!OFF! - tzdata installed successfully.
    ) else (
        echo       !ERRC!WARNING!OFF! - Could not install tzdata automatically.
        echo       Please run manually:
        echo       %%USERPROFILE%%\.dcssb\Scripts\pip install tzdata
    )
) else (
    echo       !ERRC!WARNING!OFF! - DCSServerBot Python environment not found at default location.
    echo       Please install tzdata manually:
    echo       %%USERPROFILE%%\.dcssb\Scripts\pip install tzdata
)

:: ── Copy plugin files ─────────────────────────────────────────────────────────
echo [2/4] Copying plugin files...
if not exist "!DCSSB_PATH!\plugins\jano" mkdir "!DCSSB_PATH!\plugins\jano"
if not exist "!DCSSB_PATH!\plugins\jano\db" mkdir "!DCSSB_PATH!\plugins\jano\db"

set "COPY_FAIL="
copy /Y "%SCRIPT_DIR%plugins\jano\commands.py"    "!DCSSB_PATH!\plugins\jano\commands.py"    > nul
if errorlevel 1 set "COPY_FAIL=1"
copy /Y "%SCRIPT_DIR%plugins\jano\__init__.py"    "!DCSSB_PATH!\plugins\jano\__init__.py"    > nul
if errorlevel 1 set "COPY_FAIL=1"
copy /Y "%SCRIPT_DIR%plugins\jano\listener.py"    "!DCSSB_PATH!\plugins\jano\listener.py"    > nul
if errorlevel 1 set "COPY_FAIL=1"
copy /Y "%SCRIPT_DIR%plugins\jano\version.py"     "!DCSSB_PATH!\plugins\jano\version.py"     > nul
if errorlevel 1 set "COPY_FAIL=1"
copy /Y "%SCRIPT_DIR%plugins\jano\db\tables.sql"  "!DCSSB_PATH!\plugins\jano\db\tables.sql"  > nul
if errorlevel 1 set "COPY_FAIL=1"
if defined COPY_FAIL (
    echo       !ERRC!FAILED!OFF! - Some plugin files could not be copied. Check the folder permissions and run the installer again.
) else (
    echo       !OKC!OK!OFF! - Plugin files copied.
)

:: ── Copy config file (only if it doesn't exist) ───────────────────────────────
echo [3/4] Copying configuration file...
if not exist "!DCSSB_PATH!\config\plugins\jano.yaml" (
    if not exist "!DCSSB_PATH!\config\plugins" mkdir "!DCSSB_PATH!\config\plugins"
    copy /Y "%SCRIPT_DIR%config\plugins\jano.yaml" "!DCSSB_PATH!\config\plugins\jano.yaml" > nul
    if errorlevel 1 (
        set "COPY_FAIL=1"
        echo       !ERRC!FAILED!OFF! - Could not create jano.yaml. Check the folder permissions and run the installer again.
    ) else (
        echo       !OKC!OK!OFF! - jano.yaml created. Edit it to configure your roles and timezone.
    )
) else (
    echo       SKIPPED - jano.yaml already exists, not overwritten.
    echo       Your existing configuration has been preserved.
)

:: ── Check main.yaml for jano entry ───────────────────────────────────────────
echo [4/4] Checking main.yaml...
findstr /C:"- jano" "!DCSSB_PATH!\config\main.yaml" > nul 2>&1
if !ERRORLEVEL! == 0 (
    echo       !OKC!OK!OFF! - jano already listed in main.yaml.
) else (
    echo       !ACTC!ACTION REQUIRED!OFF! - Add the following to your config\main.yaml:
    echo.
    echo           opt_plugins:
    echo             - jano
    echo.
)

:: ── Done ─────────────────────────────────────────────────────────────────────
echo.
echo ============================================================
:: Delayed expansion is switched off for this message: with it on, a literal "!" cannot be echoed
setlocal DisableDelayedExpansion
if defined COPY_FAIL (
    echo  %ERRC%Installation FAILED%OFF% - see the messages above
) else (
    echo  %NEWC%Installation complete!%OFF% Jano %NEWC%Ver. %NEW_VER%%OFF%
)
endlocal
echo ============================================================
echo.
if not defined COPY_FAIL echo !RSTC!RESTART REQUIRED!OFF! - Restart DCSServerBot to load !NEWC!Ver. !NEW_VER!!OFF! ^(the running bot keeps the old code until then^)
if not defined COPY_FAIL echo.
echo Next steps:
echo   1. Make sure 'jano' is listed under opt_plugins in config\main.yaml
echo   2. Edit config\plugins\jano.yaml to set your roles and timezone
echo   3. Restart DCSServerBot
echo   4. Use /jano setup to create your first instance
echo   Later updates: an admin can run /jano upgrade in Discord
echo.
pause
