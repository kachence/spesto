FROM python:3.12-slim

RUN useradd --uid 1000 --create-home app

WORKDIR /app

COPY pyproject.toml ./
RUN pip install --no-cache-dir .

COPY main.py ./
COPY sql ./sql

USER app

ENV PYTHONUNBUFFERED=1

CMD ["python", "main.py"]
