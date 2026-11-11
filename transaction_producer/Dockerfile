FROM python:3.12-slim

WORKDIR /app
COPY transaction_producer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY transaction_producer/main.py .

CMD ["python", "main.py"]
