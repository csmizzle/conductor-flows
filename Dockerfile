# We're using the latest version of Prefect with Python 3.10
FROM prefecthq/prefect:2-python3.10

# Add our requirements.txt file to the image and install dependencies
COPY ./requirements.txt .
RUN pip cache purge && pip install --no-cache-dir -r requirements.txt

# Add our flow code to the image
COPY ./conductor_flows ./conductor_flows

# Run our flow script when the container starts
CMD ["python", "./conductor_flows/serve.py"]
