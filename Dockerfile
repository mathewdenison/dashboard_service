# Use a base image with Python
FROM python:3.8-slim

# Install necessary packages
RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Set the working directory inside the container
WORKDIR /app

# Copy requirements.txt and install dependencies
COPY requirements.txt /app/
RUN pip install -r requirements.txt

# Copy the application code
COPY . /app

# Expose the port the app runs on
EXPOSE 8000

RUN pip install eventlet

# Set the command to run the app with gunicorn
CMD ["gunicorn", "-k", "eventlet", "-w", "1", "--timeout", "3600", "-b", "0.0.0.0:8000", "wsgi:app"]
