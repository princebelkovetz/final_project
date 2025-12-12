FROM python:3.12-slim

WORKDIR /app

RUN mkdir -p /app/logs && \
    touch /app/logs/db_writer.log && \
    chmod -R 777 /app/logs

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py .

CMD ["python", "app.py"]