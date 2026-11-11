FROM python:3.12-slim

WORKDIR /app
COPY score_writer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY score_writer/app ./app

CMD ["python", "-m", "app.main"]
