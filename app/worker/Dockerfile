
FROM python:latest

##NEED TO ADD SCRIPTS TO FOLDER
# Update pip package installer
RUN python -m pip install --upgrade pip

# Install needed Python packages
RUN python -m pip install --upgrade uproot awkward vector numpy matplotlib pika

# Set the working directory
WORKDIR /app

# Copy Python script ##MAY NEED TO REMOVE THIS WHEN SPLITTING
COPY . .

# Port mapping required? e.g. EXPOSE 4000

# Command to run the script

# CHANGE TO BE RELEVANT SCRIPT WHEN YOU HAVE THE NAMES
CMD ["python", "worker.py"]
