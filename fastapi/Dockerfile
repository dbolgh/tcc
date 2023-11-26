FROM python:3.9-slim

RUN apt-get update \
    && apt-get -y install libpq-dev gcc 

WORKDIR /app

COPY ./fastapi_app.py .
COPY ./requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# Make port 5000 available to the world outside this container
EXPOSE 8000

CMD ["uvicorn", "fastapi_app:app", "--host", "0.0.0.0", "--port", "8000"]
