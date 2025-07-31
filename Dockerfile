# Use the official Apache Airflow image as the base image
FROM apache/airflow:2.8.4

# Set the working directory to /usr/local/airflow
WORKDIR /usr/local/airflow

# Install ffmpeg and cleanup
USER root
RUN apt-get update && \
    apt-get install -y ffmpeg && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Switch back to airflow user
USER airflow

# Copy requirements
COPY ./requirements.txt /requirements.txt

# Install any additional Python packages specified in requirements.txt
RUN python3 -m pip install --upgrade pip
RUN pip3 install -r /requirements.txt
