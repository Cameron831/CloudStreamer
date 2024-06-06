import boto3
import os
from flask import Flask, request, Response
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
bucket = "test-bucket-cloud-streamer"


@app.route("/upload", methods=['POST'])
def handle_upload():
    # Retrieve a list of files from the request; expect 'files[]' to be the key in the form data
    files = request.files.getlist('files[]')

    # Check if no files were included in the request
    if not files:
        return "No files provided", 400  # Return HTTP 400 if no files are present

    results = []  # List to store the result of each file upload

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


@app.route('/stream/<filename>', methods=['GET'])
def stream_mp3(filename):
    # Get the 'Range' header from the request, which indicates the byte range the client wants to fetch
    range_header = request.headers.get('Range', None)

    if range_header:
        # If a range is specified, parse the 'Range' header value to get the start and end bytes
        byte_range = range_header.split('=')[1]
        start_byte, end_byte = byte_range.split('-')
        start_byte = int(start_byte)  # Convert the start byte to an integer
        end_byte = int(end_byte) if end_byte else None  # Convert the end byte to an integer if specified

        # Create the range header value for the S3 get_object request
        range_header_value = f'bytes={start_byte}-'
        if end_byte:
            range_header_value += str(end_byte)

        # Fetch the specified byte range from S3
        s3_response = s3.get_object(
            Bucket=bucket,
            Key=filename,
            Range=range_header_value
        )

        data = s3_response['Body'].read()  # Read the byte data from the response body
        content_length = s3_response['ContentLength']  # Get the length of the content in the response
        file_size = s3.head_object(Bucket=bucket, Key=filename)['ContentLength']  # Get the total file size

        # Calculate the end byte if it wasn't specified in the request
        end_byte = start_byte + content_length - 1 if not end_byte else end_byte

        # Return the response with the specified byte range, including necessary headers
        return Response(data, status=206, mimetype='audio/mpeg', headers={
            'Content-Range': f'bytes {start_byte}-{end_byte}/{file_size}',  # Specify the byte range being returned
            'Accept-Ranges': 'bytes',  # Indicate that the server accepts byte-range requests
            'Content-Length': str(content_length)  # Specify the length of the content being returned
        })
    else:
        # If no range is specified, return the entire file
        s3_response = s3.get_object(Bucket=bucket, Key=filename)
        data = s3_response['Body'].read()  # Read the entire file data from the response body

        # Return the full file with the necessary headers
        return Response(data, mimetype='audio/mpeg', headers={
            'Accept-Ranges': 'bytes'  # Indicate that the server accepts byte-range requests
        })


if __name__ == '__main__':
    app.run(debug=True)
