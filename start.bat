@echo off
chcp 65001 > nul
setlocal

rem start.bat - запуск MES HMI утилиты на Windows (нативный Python, не WSL)

cd /d "%~dp0"

if "%PORT%"=="" set PORT=8001
if "%HOST%"=="" set HOST=0.0.0.0
if "%APP%"==""  set APP=main_mes:app

echo == MES HMI ==
echo Папка: %CD%
echo Адрес: http://%HOST%:%PORT%
echo.

where python >nul 2>&1
if errorlevel 1 (
  echo ОШИБКА: python не найден в PATH.
  echo Установите Python 3 с python.org и включите "Add python.exe to PATH".
  pause
  exit /b 1
)

if not exist ".venv" (
  echo Создаю виртуальное окружение .venv ...
  python -m venv .venv
)

call .venv\Scripts\activate.bat

python -c "import fastapi, uvicorn, websockets" >nul 2>&1
if errorlevel 1 (
  echo Устанавливаю зависимости ...
  python -m pip install --upgrade pip >nul
  if exist requirements.txt (
    pip install -r requirements.txt
  ) else (
    pip install fastapi uvicorn pydantic websockets
  )
)

echo Запуск uvicorn %APP% на %HOST%:%PORT% ...
start "" http://127.0.0.1:%PORT%
uvicorn %APP% --host %HOST% --port %PORT%

endlocal