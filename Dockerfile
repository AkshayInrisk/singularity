FROM python:3.10-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
  && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["gunicorn","--bind","0.0.0.0:8080","--workers","1","--threads","8","--timeout","3600","--graceful-timeout","3600","--keep-alive","75","--access-logfile","-","--error-logfile","-","app:app"]

