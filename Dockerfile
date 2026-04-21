# Используем стабильный и легкий Python
FROM python:3.10-slim

# Устанавливаем рабочую директорию внутри контейнера
WORKDIR /app

# Копируем список зависимостей
COPY requirements.txt .

# Устанавливаем библиотеки
RUN pip install --no-cache-dir -r requirements.txt

# Копируем твой основной скрипт
COPY tele_send_anom.py .

# Запускаем скрипт
CMD ["python", "tele_send_anom.py"]