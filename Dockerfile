# Python Image
FROM python:3.8

# Set the working directory in the container to /app
WORKDIR /app

# Copy requirements.txt into the app directory
COPY requirements.txt .

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# This is to copy your current directory (with the source code) into the Docker image
COPY . /app

# The command to run your application when the docker container starts
CMD ["python", "./dashboard.py"]