#!/usr/bin/env bash
# start.sh — запуск MES HMI-утилиты на любом Linux (включая WSL)
# При первом запуске создаёт .venv и ставит зависимости.
# Использование:
#   ./start.sh              # запуск на 0.0.0.0:8001
#   PORT=8002 ./start.sh    # другой порт
#   HOST=127.0.0.1 ./start.sh   # только localhost

set -e

# Переходим в папку, где лежит сам скрипт (независимо от того, откуда его вызвали)
cd "$(dirname "$(readlink -f "$0")")"

HOST="${HOST:-0.0.0.0}"
PORT="${PORT:-8001}"
APP="${APP:-main_mes:app}"

echo "== MES HMI =="
echo "Папка: $(pwd)"
echo "Адрес: http://${HOST}:${PORT}"
echo

# 1. Проверяем python3
if ! command -v python3 >/dev/null 2>&1; then
  echo "ОШИБКА: python3 не найден. Установите: sudo apt install python3 python3-venv python3-pip"
  exit 1
fi

# 2. Создаём venv при первом запуске
if [ ! -d ".venv" ]; then
  echo "Создаю виртуальное окружение .venv ..."
  python3 -m venv .venv
fi

# 3. Активируем
# shellcheck disable=SC1091
source .venv/bin/activate

# 4. Ставим/обновляем зависимости, если их нет
if ! python -c "import fastapi, uvicorn, websockets" >/dev/null 2>&1; then
  echo "Устанавливаю зависимости ..."
  pip install --upgrade pip >/dev/null
  if [ -f "requirements.txt" ]; then
    pip install -r requirements.txt
  else
    pip install fastapi uvicorn pydantic websockets
  fi
fi

# 5. Запускаем
echo "Запуск uvicorn ${APP} на ${HOST}:${PORT} ..."
exec uvicorn "${APP}" --host "${HOST}" --port "${PORT}"