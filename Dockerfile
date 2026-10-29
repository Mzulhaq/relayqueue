FROM python:3.11-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app
COPY requirements.lock pyproject.toml README.md LICENSE ./
COPY src ./src
RUN pip install --no-cache-dir -r requirements.lock && pip install --no-cache-dir --no-deps . \
    && useradd --create-home --uid 10001 relayqueue
COPY scripts ./scripts
USER relayqueue
EXPOSE 8000
CMD ["uvicorn", "relayqueue.api:create_app", "--factory", "--host", "0.0.0.0", "--port", "8000"]
