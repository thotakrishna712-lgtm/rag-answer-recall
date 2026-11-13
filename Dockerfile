FROM python:3.15-slim AS base
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app
RUN useradd --create-home --uid 10001 app

COPY pyproject.toml README.md ./
COPY src/ ./src/
RUN pip install --no-cache-dir ".[kafka]"
COPY data/ ./data/
RUN mkdir -p var && chown app:app var
USER app

EXPOSE 8000
HEALTHCHECK --interval=15s --timeout=3s CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/health')"
CMD ["uvicorn", "--factory", "ragrecall.api:create_app", "--host", "0.0.0.0", "--port", "8000"]
