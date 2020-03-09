import argparse
import boto3
import botocore
from boto3.s3.transfer import TransferConfig
from botocore.config import Config
import os
import threading
import sys
import math 

ACCESS_KEY = ''
SECRET_KEY = ''
REGION_NAME = "ap-southeast-1"

class ProgressPercentage(object):
    def __init__(self, file_name, filesize):
        self._file_name = file_name
        self._size = filesize
        self._seen_so_far = 0
        self._lock = threading.Lock()

    def __call__(self, bytes_amount):
        def convertSize(size):
            if (size == 0):
                return '0B'
            size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
            i = int(math.floor(math.log(size,1024)))
            p = math.pow(1024,i)
            s = round(size/p,2)
            return '%.2f %s' % (s,size_name[i])
            
        # To simplify, assume this is hooked up to a single file_name
        with self._lock:
            self._seen_so_far += bytes_amount
            percentage = (self._seen_so_far / self._size) * 100
            sys.stdout.write(
                "\r%s  %s / %s (%.2f%%)      " % (
                    self._file_name, convertSize(self._seen_so_far), convertSize(self._size),
                    percentage))
            sys.stdout.flush()

def upload(file_name, object_name, bucket_name):
    try:
        s3_client = boto3.client(service_name="s3",
                            aws_access_key_id=ACCESS_KEY,
                            aws_secret_access_key=SECRET_KEY,
                            region_name=REGION_NAME,
                            config=Config(
                                proxies={"http":"172.24.65.79:8080", "http":"cacheproxy.hcnet.vn:8080"}
                                )
                            )
        GB = 1024 ** 3
        file_size = float(os.path.getsize(file_name))
        s3_client.upload_file(
            Filename=file_name,
            Bucket=bucket_name,
            Key=object_name,
            Callback=ProgressPercentage(file_name, file_size),
            Config=TransferConfig(
                        multipart_threshold=5*GB,
                        max_concurrency=10,
                        multipart_chunksize=5*GB,
                        use_threads=True
                        ),
            ExtraArgs={'ServerSideEncryption': 'AES256'}
            )

    except Exception as e:
        print(str(e))

def download(file_name, object_name, bucket_name):
    try:
        s3_client = boto3.client(service_name="s3",
                            aws_access_key_id=ACCESS_KEY,
                            aws_secret_access_key=SECRET_KEY,
                            region_name=REGION_NAME,
                            config=Config(
                                proxies={"https":"cacheproxy.hcnet.vn:8080"}
                                )
                            )
        GB = 1024 ** 3
        file_size = (s3_client.head_object(Bucket=bucket_name, Key=object_name))["ContentLength"]
        s3_client.download_file(
            Bucket=bucket_name, 
            Key=object_name,
            Filename=file_name, 
            Callback=ProgressPercentage(file_name, file_size),
            Config=TransferConfig(
                        multipart_threshold=5*GB,
                        max_concurrency=10,
                        multipart_chunksize=5*GB,
                        use_threads=True
                        )
            )

    except Exception as e:
        print(str(e))

if __name__ == '__main__':
    file_name = "./output.csv.gz"
    bucket_name = "itbi-services"
    object_name = "result/output.csv.gz"

    upload(file_name, object_name, bucket_name)

    # download("./download.csv.gz", object_name, bucket_name)
    # download("DC_CONTRACT.csv.gz", "owner_dwh/DC_CONTRACT/DC_CONTRACT.csv.gz", "itbi-dwh-sync-nifi")
