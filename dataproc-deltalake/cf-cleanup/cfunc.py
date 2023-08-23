import os
import sys
import json
import logging
import time
from datetime import datetime
from decimal import *
import yandexcloud
import yandex.cloud.lockbox.v1.payload_service_pb2 as payload_pb
import yandex.cloud.lockbox.v1.payload_service_pb2_grpc as payload_grpc
import boto3
from boto3.dynamodb.conditions import Attr as BotoAttr

USER_AGENT = 'deltalake-cleanup'

class WorkContext(object):
    __slots__ = ("sdk", "lbxSecretId", "docapiEndpoint", "ydbConn", "tableName", "ddbTable", "tvExp")

    def __init__(self) -> None:
        self.docapiEndpoint = ''
        self.lbxSecretId = ''
        self.tableName = 'delta_log'
        self.tvExp = Decimal(time.mktime(datetime.now().timetuple()))
        self.tvExp = Decimal(1692886876)

def handleItem(wc: WorkContext, item):
    expireTime = item.get('expireTime', Decimal('0'))
    if expireTime <= wc.tvExp: # Double check, as we received pre-filtered data from YDB
        fileName = item.get('fileName', '')
        tablePath = item.get('tablePath', '')
        if len(fileName) > 0 and len(tablePath) > 0:
            logging.info(f"Removing obsolete record for table {tablePath}, file {fileName}")
            wc.ddbTable.delete_item(Key={'fileName': fileName, 'tablePath': tablePath})

def process(wc: WorkContext):
    startKey = None
    filter = BotoAttr('expireTime').lte(wc.tvExp)
    while True:
        if startKey is None:
            result = wc.ddbTable.scan(Select='SPECIFIC_ATTRIBUTES', FilterExpression=filter,
                    ProjectionExpression='tablePath,fileName,expireTime')
        else:
            result = wc.ddbTable.scan(Select='SPECIFIC_ATTRIBUTES', FilterExpression=filter,
                    ProjectionExpression='tablePath,fileName,expireTime',
                    ExclusiveStartKey=startKey)
        if result.get('Count', 0) < 1:
            break
        for item in result.get('Items', []):
            handleItem(wc, item)
        startKey = result.get('LastEvaluatedKey', None)
        if startKey is None:
            break

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
    logging.basicConfig(level=logging.INFO)
    wc = WorkContext()
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
