FROM python:3.11

WORKDIR /app

COPY pipeline.py pipeline.py
RUN pip install pandas
RUN pip install SQLAlchemy
RUN pip install psycopg2

#CMD ["python", "pipeline.py", "https://data.austintexas.gov/api/views/9t4d-g238/rows.csv", "processed.csv"]
ENTRYPOINT [ "python", "pipeline.py", "https://data.austintexas.gov/api/views/9t4d-g238/rows.csv","processed.csv"]

