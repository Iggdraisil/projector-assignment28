import json
import boto3
from PIL import Image

client = boto3.client('s3')
formats_to_export = ['png', 'bmp', 'gif']


def file_path(path: str) -> str:
    return f'/tmp/{path}'


def lambda_handler(event: dict, _):
    for record in event['Records']:
        process_record(record)

    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }


def process_record(record):
    file_key = record['s3']['object']['key']

    print(f"converting {file_key}")

    client.download_file(record['s3']['bucket']['name'], file_key, file_path(file_key))

    with Image.open(file_path(file_key)) as image:
        upload_formatted(file_key.split('.')[0], image)


def upload_formatted(filename: str, image):
    for fmt in formats_to_export:
        full_filename = f'{filename}.{fmt}'
        with open(file_path(full_filename), 'wb') as new_image:
            image.save(new_image, format=fmt)
        client.upload_file(file_path(full_filename), f'projector-assignment28-{fmt}', full_filename)
