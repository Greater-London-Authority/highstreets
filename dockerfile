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

# Copy README.md and install Poetry with retry logic
COPY README.md /app/README.md
RUN pip install --no-cache-dir --retries 3 --timeout 60 poetry python-dotenv

# Copy only necessary files for Poetry installation first (for better caching)
COPY pyproject.toml poetry.lock ./

# Install project dependencies with retry logic and reduced parallelism
RUN poetry config installer.max-workers 1 && \
    poetry config installer.parallel false && \
    for i in 1 2 3; do \
        poetry install --no-root && break || \
        (echo "Poetry install attempt $i failed, retrying in 15 seconds..." && sleep 15); \
    done

# Copy the entire project into the container at /app
COPY . .

# Install the project in editable mode after copying all files
RUN poetry run pip install -e .

# Install additional libraries and dependencies needed for AWS
RUN pip install --retries 3 --timeout 60 psycopg2

# Set the PYTHONPATH environment variable to ensure /app is included
ENV PYTHONPATH /app

# glapy will auto-install when highstreets is imported (if GITHUB_TOKEN is available)

# Set environment variables for AWS Batch and specify the default command
CMD ["poetry", "run", "python"]