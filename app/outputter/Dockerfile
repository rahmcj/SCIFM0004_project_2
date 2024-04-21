
FROM python:latest


# Update pip package installer
RUN python -m pip install --upgrade pip

# Install needed Python packages
RUN python -m pip install --upgrade uproot awkward vector numpy matplotlib pika requests aiohttp

# Set the working directory
WORKDIR /app

# Copy Python script
COPY /outputter/output.py /app/output.py
COPY infofile.py /app/infofile.py

# Port mapping required? e.g. EXPOSE 4000

# Command to run the script
CMD ["python", "output.py"]
