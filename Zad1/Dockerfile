# Dockerfile for FastAPI
FROM python:3.9-slim

# libaries
RUN pip install fastapi uvicorn sqlalchemy psycopg2-binary

# copy project files to container
WORKDIR /app
COPY ./ /app

# port for FastAPI
EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
