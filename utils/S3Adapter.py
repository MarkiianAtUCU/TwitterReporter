import json

import boto3
from botocore.exceptions import ClientError


class S3Adapter:
    def __init__(self, config, bucket):
        self.session = boto3.Session(
            aws_access_key_id=config["aws_access_key_id"],
            aws_secret_access_key=config["aws_secret_access_key"],
            aws_session_token=config["aws_session_token"],
        )
        self.s3 = self.session.resource("s3")
        self.bucket = bucket

    def upload_file(self, filename, data):
        s3object = self.s3.Object(self.bucket, filename)
        try:
            s3object.put(
                Body=(bytes(json.dumps(data).encode('utf-8')))
            )
        except ClientError as e:
            print("ERR", e)
            return False, str(e)
        return True, "OK"
