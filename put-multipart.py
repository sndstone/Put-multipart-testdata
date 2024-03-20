import boto3
import os
import uuid
import json
import logging
from threading import Thread
from queue import Queue

# Setup logging
LOG_FILENAME = 's3_upload.log'
file_logger = logging.getLogger('file_logger')

file_handler = logging.FileHandler(LOG_FILENAME)
file_logger.addHandler(file_handler)

# Function to read credentials from JSON file
def read_credentials_from_json(file_path):
    try:
        with open(file_path, "r") as json_file:
            credentials = json.load(json_file)
            return credentials
    except Exception as e:
        file_logger.error(f'Error reading JSON: {e}')
        return None

# Logging function
def log_thread(q):
    while True:
        message = q.get()
        if message is None:
            break
        file_logger.log(message[0], message[1])
        if message[0] >= logging.INFO:
            print(f'LOG ({logging.getLevelName(message[0])}): {message[1]}')
        q.task_done()

# Asks inputs to run run the script
JSON_IMPORT = input("Do you want to import JSON file for configuration? (yes/no): ")

if JSON_IMPORT.lower() == "yes":
    JSON_FILE_PATH = input("Enter the JSON file path: ")
    credentials = read_credentials_from_json(JSON_FILE_PATH)
    BUCKET_NAME = credentials["bucket_name"]
    S3_ENDPOINT_URL = credentials["s3_endpoint_url"]
    AWS_ACCESS_KEY_ID = credentials["aws_access_key_id"]
    AWS_SECRET_ACCESS_KEY = credentials["aws_secret_access_key"]
else:
    BUCKET_NAME = input("Enter the bucket name: ")
    S3_ENDPOINT_URL = input("Enter the S3 endpoint URL, (EXAMPLE http://example.com:443): ")
    AWS_ACCESS_KEY_ID = input("Enter the AWS access key ID: ")
    AWS_SECRET_ACCESS_KEY = input("Enter the AWS secret access key: ")

def get_integer_input(prompt):
    while True:
        try:
            return int(input(prompt))
        except ValueError:
            print("Please enter a valid integer.")

OBJECT_SIZE = get_integer_input("Enter the size of the objects in bytes: ")
PARTS_COUNT = get_integer_input("Enter the number of parts for multi-part upload: ")
OBJECTS_COUNT = get_integer_input("Enter the number of objects to be placed: ")
OBJECT_PREFIX = input("Enter the prefix for the objects: ")

# Set logging level
LOG_LEVEL = input("Enter the logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL): ")
LOG_LEVELS = {'DEBUG': logging.DEBUG, 'INFO': logging.INFO, 'WARNING': logging.WARNING, 'ERROR': logging.ERROR, 'CRITICAL': logging.CRITICAL}
file_logger.setLevel(LOG_LEVELS.get(LOG_LEVEL.upper()))

# Create an S3 client
s3 = boto3.client("s3",
                  verify=False,
                  aws_access_key_id=AWS_ACCESS_KEY_ID,
                  aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                  endpoint_url=S3_ENDPOINT_URL)

# Define a function that creates a single object in the bucket
def create_object(arg, q):
    try:
        # Generate a random object key with the prefix
        object_key = f"{OBJECT_PREFIX}{str(uuid.uuid4())}"
        q.put((logging.INFO, f"Generated object key: {object_key}"))

        # Generate random content for the object
        object_content = os.urandom(OBJECT_SIZE)
        q.put((logging.DEBUG, f"Generated object content of size: {len(object_content)}"))

        # Start multi-part upload
        mpu = s3.create_multipart_upload(Bucket=BUCKET_NAME, Key=object_key)
        q.put((logging.INFO, f"POST - Initiated multi-part upload for object {object_key}"))
        parts = []

        # Upload parts
        part_size = OBJECT_SIZE // PARTS_COUNT
        for i in range(PARTS_COUNT):
            part_content = object_content[i*part_size:(i+1)*part_size]
            part = upload_part(i, part_content, mpu['UploadId'], object_key)
            parts.append({'PartNumber': i+1, 'ETag': part['ETag']})
            q.put((logging.INFO, f"PUT - Uploaded part {(i+1)} of size {part_size} for object {object_key}"))

        # Complete multi-part upload
        result = s3.complete_multipart_upload(Bucket=BUCKET_NAME, Key=object_key, UploadId=mpu['UploadId'],
                                              MultipartUpload={'Parts': parts})
        q.put((logging.INFO, f"POST - Completed multi-part upload for object {object_key}"))

        # Log the response
        q.put((logging.DEBUG, f"Response: {result}"))

        # Return the response
        return result

    except Exception as e:
        file_logger.error(f'Error creating object: {e}')
        return None

def upload_part(i, part_content, upload_id, object_key):
    return s3.upload_part(Bucket=BUCKET_NAME, Key=object_key, PartNumber=i+1,
                          UploadId=upload_id, Body=part_content)

# Create a queue for the logging thread
q = Queue()

# Start the logging thread
Thread(target=log_thread, args=(q,), daemon=True).start()

# Use a loop to create objects in the bucket
responses = []
for i in range(OBJECTS_COUNT):
    responses.append(create_object(i, q))

# Signal the logging thread to finish
q.put(None)

# Wait for all log messages to be processed
q.join()

# Iterate through the responses and print the information
for response in responses:
    if response is not None:
        print(f"HTTP Status Code: {str(response['ResponseMetadata']['HTTPStatusCode'])}")
        print(f"Request ID: {response['ResponseMetadata']['RequestId']}")
        print(f"Host ID: {response['ResponseMetadata']['HostId']}")

print(f"Logs have been written to {LOG_FILENAME}")
print("Test completed.")
