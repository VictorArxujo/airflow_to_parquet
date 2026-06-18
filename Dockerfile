FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV TZ=America/Sao_Paulo

RUN apt-get update && apt-get install -y cron tzdata && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN mkdir -p /app/logs /app/data/dados_parquet

# Agenda o arquivamento diário (horários em America/Sao_Paulo).
# O script é idempotente e faz backfill, então uma 2ª execução é só rede de segurança.
RUN echo "11 40 * * * root cd /app && /usr/local/bin/python /app/app.py >> /app/logs/cron.log 2>&1" > /etc/cron.d/arquivamento && \
    echo "" >> /etc/cron.d/arquivamento && \
    chmod 0644 /etc/cron.d/arquivamento

# Exporta as variáveis do container (env_file) para o ambiente do cron e roda cron em foreground.
CMD ["sh", "-c", "env >> /etc/environment && cron -f"]
