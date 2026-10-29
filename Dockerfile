FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONUTF8=1 \
    PORT=8080

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt
COPY shared ./shared
COPY aggregator_service ./aggregator_service
CMD ["sh", "-c", "exec uvicorn aggregator_service.app:app --host 0.0.0.0 --port ${PORT:-8080}"]
