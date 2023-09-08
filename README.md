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

2. **Build the Docker Image**:

   Build the Docker image using the provided `Dockerfile`.

   ```bash
   docker build -t pipeline:0.1 .
   ```

   Replace `pipeline:0.1` with your desired image name.

## Usage

Once you have built the Docker image, you can run it as a container.

```bash
docker run -it --mount source=myvol_data1,target=/app pipeline:0.1
```

This command will start a container and open a python bash where you can run the command. This command will also create a mount on the docker container with the name myvol_data1 and store the required output files 

```bash
python pipeline_c.py https://data.austintexas.gov/api/views/9t4d-g238/rows.csv processed.csv
```

This will run the pipeline code which :
    1. reads the csv from the link provided in the first argurment
    2. create an output file with the name processed.csv(provided in the second argument)

## Stopping and Cleaning Up

To stop the running container, you can use the following command:

```bash
docker stop <container_id>
```

Replace `<container_id>` with the actual container ID or use the container name.

To remove the container and clean up resources (including the associated image), use:

```bash
docker rm <container_id>
docker rmi simple-docker-image
```

## Customization

You can customize this Docker image by modifying the source code in the repository. Here are some common customization options:

- **Changing the Application**: Replace the content in the `app` directory with your own application code.

- **Modifying the Dockerfile**: If you have specific requirements or dependencies, you can modify the `Dockerfile`. For example, you can install additional software packages or set environment variables.

## Contributing

If you'd like to contribute to this project, feel free to open issues or submit pull requests on the GitHub repository. Contributions and improvements are always welcome.
