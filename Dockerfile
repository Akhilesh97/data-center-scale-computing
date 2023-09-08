FROM python:3.11

WORKDIR /app
COPY pipeline.py pipeline_c.py
RUN pip install pandas



ENTRYPOINT  ["bash"]

