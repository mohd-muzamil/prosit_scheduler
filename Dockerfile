# Use the official Apache Airflow image as the base image
FROM apache/airflow:2.8.4

# Set the working directory to /usr/local/airflow
WORKDIR /usr/local/airflow

# Copy your requirements.txt file into the image
# COPY requirements.txt .
COPY ./requirements.txt /requirements.txt


# Install any additional Python packages specified in requirements.txt
RUN python3 -m pip install --upgrade pip
RUN pip3 install -r /requirements.txt

# Optional: You can add more customizations here if needed

# Set environment variables (if needed)
#ENV MY_ENV_VARIABLE=value

# Continue with the original Dockerfile instructions from apache/airflow image

# ...

# Start the Airflow scheduler (or any other desired command)
#CMD ["scheduler"]

