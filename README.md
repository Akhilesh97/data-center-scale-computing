# Airflow ETL Pipeline

This project sets up an Airflow ETL (Extract, Transform, Load) pipeline using Docker Compose. The pipeline fetches data from a given URL, processes it, and loads it into a local Postgres database.

## Getting Started

These instructions will help you set up and run the ETL pipeline on your local machine.

### Prerequisites

Make sure you have Docker and Docker Compose installed on your machine.

### Installing

1. Clone this repository:

    ```bash
    git clone https://github.com/Akhilesh97/data-center-scale-computing.git
    cd your-repo
    ```

2. Build the Airflow image and start the Docker containers:

    ```bash
    docker-compose up
    ```

3. Access the Airflow web interface at [http://localhost:8080](http://localhost:8080) and configure the necessary connections.

4. Run the Airflow DAG to initiate the ETL process.

## ETL Process Overview

1. **Build Airflow Image and PostgreSQL Service:**

    The Docker Compose file sets up an Airflow instance and a PostgreSQL service. The necessary tables are created using the `init.sql` script.

2. **Extract Data:**

    The DAG's "extract" task fetches raw data from the URL [https://data.austintexas.gov/resource/9t4d-g238.json](https://data.austintexas.gov/resource/9t4d-g238.json) and writes it to a GCP S3 bucket based on the current timestamp.

3. **Transform Data:**

    The "transform" task reads the data from the S3 bucket, applies transformation logic to create dimension and fact tables, and writes them as Parquet files to a separate S3 bucket organized by the current date folder.

4. **Load Data:**

    The "load" task retrieves the transformed data from the S3 bucket and writes it to the local Postgres instance.

## Usage

1. Access the Airflow web interface: [http://localhost:8080](http://localhost:8080)
2. Trigger the DAG to start the ETL process.

## Contributing

Feel free to contribute to this project by opening issues or creating pull requests.

## License

This project is licensed under the [MIT License](LICENSE).
