
FROM python:latest

# Set the working directory
WORKDIR /app

# Update pip package installer
RUN python -m pip install --upgrade pip

# Install needed Python packages
RUN python -m pip install --upgrade uproot awkward vector numpy matplotlib pika requests aiohttp

# Copy Python script 
COPY producer/producer.py /app/producer.py
COPY infofile.py /app/infofile.py

# Port mapping required? e.g. EXPOSE 4000

# Command to run the script
CMD ["python", "producer.py"]
