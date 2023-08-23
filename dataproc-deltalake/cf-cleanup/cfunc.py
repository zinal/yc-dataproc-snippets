import os
import sys
import json
import logging
import time
import yandexcloud
import yandex.cloud.lockbox.v1.payload_service_pb2 as payload_pb
import yandex.cloud.lockbox.v1.payload_service_pb2_grpc as payload_grpc
import boto3

USER_AGENT = 'deltalake-cleanup'

class WorkContext(object):
    __slots__ = ("sdk", "lbxSecretId", "docapiEndpoint", "ydbConn", "tableName", "ddbTable")

def process(wc: WorkContext):
    startKey = None
    while True:
        if startKey is None:
            result = wc.ddbTable.scan(Select='SPECIFIC_ATTRIBUTES',
                    ProjectionExpression='tablePath,fileName,expireTime')
        else:
            result = wc.ddbTable.scan(Select='SPECIFIC_ATTRIBUTES',
                    ProjectionExpression='tablePath,fileName,expireTime',
                    ExclusiveStartKey=startKey)
        if result['Count'] < 1:
            break
        for item in result['Items']:
            print(item)
        startKey = result.LastEvaluatedKey

def get_key_id(creds):
    for e in creds.entries:
        if e.key == "key-id":
            return e.text_value
    return ''

def get_key_secret(creds):
    for e in creds.entries:
        if e.key == "key-secret":
            return e.text_value
    return ''

def run(wc: WorkContext):
    payloadSvc = wc.sdk.client(payload_grpc.PayloadServiceStub)
    credsData = payloadSvc.Get(payload_pb.GetPayloadRequest(secret_id = wc.lbxSecretId))
    wc.ydbConn = boto3.resource('dynamodb',
            endpoint_url = wc.docapiEndpoint,
            aws_access_key_id = get_key_id(credsData),
            aws_secret_access_key = get_key_secret(credsData))
    wc.ddbTable = wc.ydbConn.Table(wc.tableName)
    print(dir(wc.ddbTable))
    process(wc)

def handler(event, context):
    logging.getLogger().setLevel(logging.INFO)
    wc = WorkContext()
    wc.docapiEndpoint = os.getenv("DOCAPI_ENDPOINT")
    wc.lbxSecretId = os.getenv("LBX_SECRET_ID")
    wc.tableName = os.getenv("TABLE_NAME")
    wc.sdk = yandexcloud.SDK(user_agent=USER_AGENT)
    run(wc)

# python3 cf-cleanup/cfunc.py docapiEndpoint lbxSecretId delta_log /Users/mzinal/Magic/key-ydb-sa1.json
if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    wc = WorkContext()
    wc.docapiEndpoint = ''
    wc.lbxSecretId = ''
    wc.tableName = 'delta_log'
    yc_sa_key_filename = 'sakey.json'
    if len(sys.argv) > 1:
        wc.docapiEndpoint = sys.argv[1]
    if len(sys.argv) > 2:
        wc.lbxSecretId = sys.argv[2]
    if len(sys.argv) > 3:
        wc.tableName = sys.argv[3]
    if len(sys.argv) > 4:
        yc_sa_key_filename = sys.argv[4]
    with open(yc_sa_key_filename) as infile:
        wc.sdk = yandexcloud.SDK(service_account_key=json.load(infile), user_agent=USER_AGENT)
    run(wc)
