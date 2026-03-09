import json
import boto3
import os
import urllib.parse

sns = boto3.client("sns")

LARGE_FILE_TOPIC = os.environ["LARGE_FILE_TOPIC"]
DATA_TEAM_TOPIC = os.environ["DATA_TEAM_TOPIC"]
FINANCE_TEAM_TOPIC = os.environ["FINANCE_TEAM_TOPIC"]

def lambda_handler(event, context):

    detail = event.get("detail", {})

    bucket = detail.get("bucket", {}).get("name")
    key = detail.get("object", {}).get("key")
    size = detail.get("object", {}).get("size", 0)
    event_time = event.get("time")

    if key:
        key = urllib.parse.unquote_plus(key)

    message = {
        "bucket": bucket,
        "file_name": key,
        "file_size": size,
        "event_time": event_time
    }

    if size > 5000000:
        sns.publish(
            TopicArn=LARGE_FILE_TOPIC,
            Message=json.dumps(message),
            Subject="Large File Uploaded"
        )

    if key and key.endswith(".csv"):
        sns.publish(
            TopicArn=DATA_TEAM_TOPIC,
            Message=json.dumps(message),
            Subject="New CSV Dataset Uploaded"
        )

    if key and key.startswith("finance/"):
        sns.publish(
            TopicArn=FINANCE_TEAM_TOPIC,
            Message=json.dumps(message),
            Subject="Finance File Uploaded"
        )

    return {
        "statusCode": 200,
        "body": json.dumps("Notifications processed")
    }



