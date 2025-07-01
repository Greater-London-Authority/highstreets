# Use a lightweight Python base image
FROM python:3.9-slim

# Install necessary system dependencies, including git and PostgreSQL development libraries
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    git \
    && rm -rf /var/lib/apt/lists/*

# Set the working directory in the container
WORKDIR /app

# Copy README.md and install Poetry
COPY README.md /app/README.md
RUN pip install --no-cache-dir poetry python-dotenv

# Copy only necessary files for Poetry installation first (for better caching)
COPY pyproject.toml poetry.lock ./

# Install project dependencies without installing the project itself
# Adding `--no-root` flag ensures Poetry only installs dependencies without installing the project itself.
RUN poetry install --no-root

# # Install glapy separately using environment variable for GitHub token
# # This ARG will be passed during docker build
# ARG GITHUB_TOKEN
# ENV GITHUB_TOKEN=${GITHUB_TOKEN}

# # Install glapy using the environment variable
# RUN poetry run pip install git+https://${GITHUB_TOKEN}@github.com/Greater-London-Authority/glapy@feature/lds-update-data

# Copy the entire project into the container at /app
COPY . .

# Install the project in editable mode after copying all files
RUN poetry run pip install -e .

# Install additional libraries and dependencies needed for AWS
RUN pip install psycopg2

# Set the PYTHONPATH environment variable to ensure /app is included
ENV PYTHONPATH /app

# Set environment variables for AWS Batch and specify the default command
# Use poetry run as the entrypoint to ensure dependencies load correctly
CMD ["poetry", "run", "python"]
# ENTRYPOINT ["poetry", "run", "python", "highstreets/aws_pipeline/msoa_e2e.py"]
