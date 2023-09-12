FROM python:3.11

WORKDIR /app
COPY pipeline.py pipeline_c.py
RUN pip install pandas

CMD ["python", "pipeline_c.py", "https://data.austintexas.gov/api/views/9t4d-g238/rows.csv", "processed.csv"]
#ENTRYPOINT  ["bash"]

