# Use the official Python image from the Docker Hub with Alpine base image
FROM python:3.11-alpine

# Set the working directory inside the container
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install any necessary dependencies or packages
# This will install dependencies from a requirements.txt file
RUN pip install --no-cache-dir -r requirements.txt

# Expose the port your app will run on (default for Flask is 5000, for example)

# Command to run the app (replace with your app's entry point)
CMD ["python", "./main.py"]