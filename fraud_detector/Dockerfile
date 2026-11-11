FROM python:3.12-slim

WORKDIR /app
COPY fraud_detector/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY common ./common
COPY fraud_detector/app ./app

CMD ["python", "-m", "app.main"]
