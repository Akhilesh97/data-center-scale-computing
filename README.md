# Airflow ETL Pipeline

This project sets up an Airflow ETL (Extract, Transform, Load) pipeline using Docker Compose. The pipeline fetches data from a given URL, processes it, and loads it into a local Postgres database.

## Getting Started

These instructions will help you set up and run the ETL pipeline on your local machine.

### Prerequisites

Make sure you have Docker and Docker Compose installed on your machine.

### Installing

1. Clone this repository:

    ```bash
    git clone https://github.com/your-username/your-repo.git
    cd your-repo
    ```

2. Build the Airflow image and start the Docker containers:

    ```bash
    docker-compose up
    ```

3. Access the Airflow web interface at [http://localhost:8080](http://localhost:8080) and configure the necessary connections.

4. Create GCP Service Account and Obtain Credentials:

    - Navigate to the Google Cloud Console: [Google Cloud Console](https://console.cloud.google.com/).
    - Open the "IAM & Admin" section: In the left-hand navigation menu, click on "IAM & Admin" and then select "Service accounts."
    - Select or create a service account:
        - If you already have a service account that you want to use, find it in the list and click on its name.
        - If you need to create a new service account, click on the "Create Service Account" button. Follow the prompts to give it a name, specify the role (permissions), and create the account.
    - Generate a keyfile:
        - Once you are viewing the details of the service account, click on the "Add Key" dropdown.
        - Choose the JSON option to create a key in JSON format.
        - This will download a JSON file containing the credentials for your service account.

    - Set the environment variable:
        - Set the `GOOGLE_APPLICATION_CREDENTIALS` environment variable to point to the path of the downloaded JSON key file. This step is important for authenticating your applications with Google Cloud services.

5. Run the Airflow DAG to initiate the ETL process.

## ETL Process Overview

1. **Build Airflow Image and PostgreSQL Service:**

    The Docker Compose file sets up an Airflow instance and a PostgreSQL service. The necessary tables are created using the `init.sql` script.

2. **Extract Data:**

    The DAG's "extract" task fetches raw data from the URL [https://data.austintexas.gov/resource/9t4d-g238.json](https://data.austintexas.gov/resource/9t4d-g238.json) and writes it to a GCP S3 bucket based on the current timestamp.

3. **Transform Data:**

    The "transform" task reads the data from the S3 bucket, applies transformation logic to create dimension and fact tables, and writes them as Parquet files to a separate S3 bucket organized by the current date folder.

4. **Load Data:**

    The "load" task retrieves the transformed data from the S3 bucket and writes it to the local Postgres instance. There are inividual load tasks for loading data into 5 diemension tables and a fact table

## Directed Acyclcic Graph of the ETL pipeline

![DAG-ETL](https://github.com/Akhilesh97/data-center-scale-computing/blob/HW3/docs/dag.png)

## Usage

1. Access the Airflow web interface: [http://localhost:8080](http://localhost:8080)
2. Trigger the DAG to start the ETL process.

## Contributing

Feel free to contribute to this project by opening issues or creating pull requests.

## License

This project is licensed under the [MIT License](LICENSE).
