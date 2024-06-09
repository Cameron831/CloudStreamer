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
    # Retrieve the folder path from the form data; expect 'folder' to be the key
    folder = request.form.get('folder', '')  # Default to root directory if no folder is specified

    # Check if no files were included in the request
    if not files:
        return "No files provided", 400  # Return HTTP 400 if no files are present

    results = []  # List to store the result of each file upload

    # Process each file in the list
    for file in files:
        filename = secure_filename(file.filename)  # Secure the filename
        if folder:
            # Prepend the folder path if provided, ensuring it ends with '/'
            folder = folder.rstrip('/') + '/'  # Normalize folder path
            s3_key = f"{folder}{filename}"
        else:
            s3_key = filename  # If no folder specified, use just the filename

        try:
            # Save the file temporarily
            filepath = f"/temp/{filename}"
            file.save(filepath)

            # Upload the file from the local system to AWS S3
            s3.upload_file(filepath, bucket, s3_key)

            # Record a successful upload result
            results.append(f"{s3_key} uploaded successfully")
        except Exception as e:
            # Record an error if the upload fails
            results.append(f"Failed to upload {s3_key}: {str(e)}")

    # Return a JSON response with the results of all file uploads
    return {"results": results}, 200


@app.route('/folders', methods=['GET'])
def get_folders():
    # Call the AWS S3 service to list objects within a specified bucket.
    # The list_objects_v2 is a method to retrieve the S3 objects.
    # 'Bucket' specifies the S3 bucket from which to list the objects.
    # 'Delimiter' is used to collapse all keys that contain the same string between the prefix and the first occurrence of the delimiter into a single result element.
    response = s3.list_objects_v2(
        Bucket=bucket,
        Delimitex='/',
    )

    # 'CommonPrefixes' contains all of the keys that are common prefixes
    # under the given bucket and delimiter. It's used to simulate a folder structure.
    # Here, it extracts the 'Prefix' from each of the common prefixes,
    # which represents each 'folder' in the S3 bucket.
    folders = [item['Prefix'] for item in response.get('CommonPrefixes', [])]

    # Returns the list of 'folders' as the response. Each 'folder' is essentially
    # a prefix under which objects are stored in the S3 bucket.
    return folders


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
