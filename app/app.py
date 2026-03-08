import json
import boto3
import os

sns = boto3.client("sns")
TOPIC_ARN = os.environ["TOPIC_ARN"]

def lambda_handler(event, context):

    detail = event.get("detail", {})

    bucket = detail.get("bucket", {}).get("name")
    key = detail.get("object", {}).get("key")
    size = detail.get("object", {}).get("size", 0)
    event_time = event.get("time")
    event_name = detail.get("reason", "Object Event")

    message = {
        "bucket": bucket,
        "file_name": key,
        "file_size": size,
        "event_time": event_time,
        "event_type": event_name
    }

    sns.publish(
        TopicArn=TOPIC_ARN,
        Message=json.dumps(message),
        Subject="S3 File Upload Event"
    )

    return {
        "statusCode": 200,
        "body": json.dumps("Notification sent")
    }

