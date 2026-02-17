import boto3
import csv
import io

s3 = boto3.client('s3')


def lambda_handler(event, context):

    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']

    # Only process files in raw/
    if not key.startswith("raw/"):
        return

    # Read file
    response = s3.get_object(Bucket=bucket, Key=key)
    content = response['Body'].read().decode('utf-8')
    csv_file = io.StringIO(content)

    reader = list(csv.reader(csv_file))

    # Simple validation: at least 2 rows (header + 1 data row)
    reader = list(csv.reader(csv_file))

    # Remove completely empty rows
    data_rows = [row for row in reader[1:]
                 if any(cell.strip() for cell in row)]

    if len(data_rows) == 0:
        raise Exception("No valid data rows found")

    # Write to clean/
    clean_key = key.replace("raw/", "clean/")
    s3.put_object(Bucket=bucket, Key=clean_key, Body=content)

    return {"status": "File validated and moved"}
