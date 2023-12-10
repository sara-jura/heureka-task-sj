FROM python:3.10
# Or any preferred Python version.
COPY . .
RUN pip install pika psycopg2
CMD ["python", "./extraction_worker.py"]