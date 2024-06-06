import boto3
import os
from flask import Flask, request
from flask_cors import CORS
from werkzeug.utils import secure_filename
from dotenv import load_dotenv

app = Flask(__name__)
CORS(app)
load_dotenv()

session = boto3.Session(
    aws_access_key_id = os.getenv("KEY_ID"),
    aws_secret_access_key = os.getenv("ACCESS_KEY"),
    region_name='us-west-1'
)
s3 = session.client('s3')

@app.route("/")
def hello_world():
    return "<p>Hello, World!</p>"

@app.route("/upload", methods=['POST'])
def handle_upload():
    # Retrieve a list of files from the request; expect 'files[]' to be the key in the form data
    files = request.files.getlist('files[]')

    # Check if no files were included in the request
    if not files:
        return "No files provided", 400  # Return HTTP 400 if no files are present

    results = []  # List to store the result of each file upload
    bucket = "test-bucket-cloud-streamer"  # AWS S3 bucket where files will be uploaded

    # Process each file in the list
    for file in files:
        print(f"uploading {file.filename}")  # Debug print to console

        try:
            # Secure the filename to ensure it's safe to use as a file name on the filesystem
            filename = secure_filename(file.filename)
            # Construct the full local path where the file will be saved temporarily
            filepath = f"/temp/{filename}"  # Make sure this directory exists and is writable

            # Save the file locally at the designated path
            file.save(filepath)

            # Upload the file from the local system to AWS S3
            s3.upload_file(filepath, bucket, filename)

            # Record a successful upload result
            results.append(f"{filename} uploaded successfully")
        except Exception as e:
            # Record an error if the upload fails
            results.append(f"Failed to upload {filename}: {str(e)}")

    # Return a JSON response with the results of all file uploads
    return {"results": results}, 200


if __name__ == "__main__":
    app.run()

