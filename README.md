## Table of Contents

- [Prerequisites](#prerequisites)
- [Getting Started](#getting-started)
- [Usage](#usage)
- [Stopping and Cleaning Up](#stopping-and-cleaning-up)
- [Customization](#customization)
- [Contributing](#contributing)

## Prerequisites

Before you begin, ensure you have the following installed on your system:

- Docker: You can download and install Docker from [Docker's official website](https://www.docker.com/get-started).

## Getting Started

1. **Clone or Download the Repository**:

   Clone this repository to your local machine or download and extract the ZIP file.

   ```bash
   git clone https://github.com/Akhilesh97/data-center-scale-computing.git
   cd data-center-scale-computing
   ```

2. **Build from the Docker compose file**:

   Build the Docker image using the provided `Dockerfile`.

   ```bash
   docker compose up --build
   ```

## Usage

Once you have built the Docker image, two services will fire up.

1. The postgres db service
   - This gets the postgres version 16 image
   - sets the environment variables such as the db username, password and the database name
   - creates a volume in the docker container to store the database
   - executes an init.sql which creates the tables required for hosting the data warehouse
2. The Extract, Transform and Load(ETL) service that
   - build from the root directory
   - extracts the raw csv file located on a remote link
   - cleans the csv
   - creates dataframes based on the data model
   - inserts into the postgres db
     
## Stopping and Cleaning Up

To stop the running services, you can use the following command:

```bash
 docker compose down
```

## The third normal form data model used to create the data warehouse

![alt text](https://github.com/Akhilesh97/data-center-scale-computing/blob/HW2/docs/ER_diagram.png?raw=true)


## Customization

You can customize this Docker image by modifying the source code in the repository. Here are some common customization options:

- **Changing the Application**: Replace the content in the `app` directory with your own application code.

- **Modifying the Dockerfile**: If you have specific requirements or dependencies, you can modify the `Dockerfile`. For example, you can install additional software packages or set environment variables.

## Contributing

If you'd like to contribute to this project, feel free to open issues or submit pull requests on the GitHub repository. Contributions and improvements are always welcome.
