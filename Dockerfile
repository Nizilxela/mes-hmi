# Dockerfile — образ MES HMI утилиты
# Базовый образ: официальный Python 3.12 (slim = минимальный размер)
FROM python:3.12-slim

# Не писать .pyc-файлы и не буферизовать stdout (важно для логов uvicorn)
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Рабочая папка внутри контейнера
WORKDIR /app

# Сначала копируем только requirements — так Docker кэширует установку зависимостей
# и не переустанавливает их при каждом изменении main_mes.py
COPY requirements.txt .

# Устанавливаем зависимости
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Теперь копируем код утилиты
COPY main_mes.py .

# Открываем порт 8001 (информативно, реальный проброс делается при docker run)
EXPOSE 8001

# Запуск сервера
CMD ["uvicorn", "main_mes:app", "--host", "0.0.0.0", "--port", "8001"]