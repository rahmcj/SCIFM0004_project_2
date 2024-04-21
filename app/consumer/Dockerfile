
FROM python:latest


# Update pip package installer
RUN python -m pip install --upgrade pip

# Install needed Python packages
RUN python -m pip install --upgrade uproot awkward vector numpy matplotlib pika requests aiohttp

# Set the working directory
WORKDIR /app

# Port mapping required? e.g. EXPOSE 4000

# Copy scripts into working directory
COPY consumer/consumer.py /app/consumer.py
COPY infofile.py /app/infofile.py

# Command to run the script
CMD ["python", "consumer.py"]
