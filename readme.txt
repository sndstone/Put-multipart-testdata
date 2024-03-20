S3 Multi-part Upload Script
This script provides a way to perform multi-part uploads to an S3 bucket using the Boto3 Python library. It allows you to specify the number of objects to be placed, the size of the objects, and the number of parts for each object. It also logs all the operations to a file and prints some of them to the console.

Prerequisites
Python 3.6 or higher
Boto3 library installed (pip install boto3)
How to use
Clone the repository.
Run the script python s3_upload.py.
When prompted, enter the necessary information:
If you want to import a JSON file for configuration, enter "yes" and provide the path to the JSON file. The JSON file should have the following structure:
{
    "bucket_name": "<your_bucket_name>",
    "s3_endpoint_url": "<your_s3_endpoint_url>",
    "aws_access_key_id": "<your_aws_access_key_id>",
    "aws_secret_access_key": "<your_aws_secret_access_key>"
}
If you don't want to use a JSON file, enter "no" and provide the necessary information when prompted.
Enter the size of the objects in bytes.
Enter the number of parts for multi-part upload.
Enter the number of objects to be placed.
Enter the prefix for the objects.
Enter the logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL).
Logging
The script logs all the operations to a file named s3_upload.log and prints some of them to the console. The log level can be set when running the script.

