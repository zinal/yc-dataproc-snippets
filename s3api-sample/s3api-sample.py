import os
import json
import sys
import yandexcloud
from yandex.cloud.storage.v1.bucket_service_pb2 import ListBucketsRequest
from yandex.cloud.storage.v1.bucket_service_pb2 import ListBucketsResponse
from yandex.cloud.storage.v1.bucket_service_pb2_grpc import BucketServiceStub

USER_AGENT_SDK = "yc-s3api-sample:v1.0"

def main():
    saFile = "tfadmin.json"
    folderId = "b1gnh4ic2up67njae68e"
    sdk = None
    with open(saFile) as f:
        sdk = yandexcloud.SDK(service_account_key=json.load(f), user_agent=USER_AGENT_SDK)
    bucketService = sdk.client(BucketServiceStub)
    bucketList = bucketService.List(ListBucketsRequest(folder_id=folderId))
    for bucket in bucketList.buckets:
        print("** %s -> %s" % (bucket.id, bucket.name))

if __name__ == '__main__':
    main()
