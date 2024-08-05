
@echo off
REM Batch file to run the Python script with client argument

REM Set the virtual environment and script paths
set "VENV_PATH=C:\Users\miroslaw-steblik\my-python-projects\white\rebalancing\venv"
set "SCRIPT_PATH=C:\Users\miroslaw-steblik\my-python-projects\white\rebalancing\src\rebalancing\main.py"


REM Check if an argument is provided
if "%1"=="" (
    echo Usage: run_pipeline.bat client_name
    exit /b 1
)

REM Set the client name from the first argument
set CLIENT_NAME=%1

REM Activate the virtual environment
call "%VENV_PATH%\Scripts\activate"

REM Run the Python script with the client argument
python "%SCRIPT_PATH%" --client %CLIENT_NAME%

pause

deactivate
