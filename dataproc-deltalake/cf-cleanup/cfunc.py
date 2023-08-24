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
    __slots__ = ("sdk", "lbxSecretId",
                 "awsRegionId", "awsKeyId", "awsKeySecret",
                 "s3PrefixFile",
                 "docapiEndpoint", "tableName", "tvExpDdb",
                 "ydbConn", "ddbTable")

    def __init__(self) -> None:
        self.lbxSecretId = ''
        self.awsRegionId = 'ru-central1'
        self.s3PrefixFile = ''
        self.docapiEndpoint = ''
        self.tableName = 'delta_log'
        self.tvExpDdb = Decimal(time.mktime(datetime.now().timetuple()))

def handleDdbItem(wc: WorkContext, item):
    expireTime = item.get('expireTime', Decimal('0'))
    if expireTime <= wc.tvExpDdb: # Extra check, we received pre-filtered data from YDB
        fileName = item.get('fileName', '')
        tablePath = item.get('tablePath', '')
        if len(fileName) > 0 and len(tablePath) > 0:
            logging.info(f"Removing obsolete record for table {tablePath}, file {fileName}")
            wc.ddbTable.delete_item(Key={'fileName': fileName, 'tablePath': tablePath})

def processDdb(wc: WorkContext):
    startKey = None
    totalCount = 0
    filter = BotoAttr('expireTime').lte(wc.tvExpDdb)
    while True:
        if startKey is None:
            result = wc.ddbTable.scan(Select='SPECIFIC_ATTRIBUTES', FilterExpression=filter,
                    ProjectionExpression='tablePath,fileName,expireTime')
        else:
            result = wc.ddbTable.scan(Select='SPECIFIC_ATTRIBUTES', FilterExpression=filter,
                    ProjectionExpression='tablePath,fileName,expireTime',
                    ExclusiveStartKey=startKey)
        localCount = result.get('Count', 0)
        if localCount < 1:
            break
        totalCount += localCount
        for item in result.get('Items', []):
            handleDdbItem(wc, item)
        startKey = result.get('LastEvaluatedKey', None)
        if startKey is None:
            break
    return totalCount

def runDdb(wc: WorkContext):
    logging.info(f"Cleanup of table {wc.tableName} in {wc.docapiEndpoint}")
    wc.ydbConn = boto3.resource('dynamodb',
            region_name = wc.awsRegionId,
            endpoint_url = wc.docapiEndpoint,
            aws_access_key_id = wc.awsKeyId,
            aws_secret_access_key = wc.awsKeySecret)
    wc.ddbTable = wc.ydbConn.Table(wc.tableName)
    logging.info("Connected, processing...")
    totalCount = processDdb(wc)
    logging.info(f"Processed {totalCount} records.")

def runS3(wc: WorkContext):
    True

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

def runAny(sdk: yandexcloud.SDK, mode: str):
    wc = WorkContext()
    wc.sdk = sdk
    wc.lbxSecretId = os.getenv("LBX_SECRET_ID")
    payloadSvc = wc.sdk.client(payload_grpc.PayloadServiceStub)
    credsData = payloadSvc.Get(payload_pb.GetPayloadRequest(secret_id = wc.lbxSecretId))
    wc.awsKeyId = get_key_id(credsData)
    wc.awsKeySecret = get_key_secret(credsData)
    if len(mode)==0:
        mode = "ddb"
    if mode == "ddb":
        wc.docapiEndpoint = os.getenv("DOCAPI_ENDPOINT")
        wc.tableName = os.getenv("TABLE_NAME")
        runDdb(wc)
    else:
        wc.s3PrefixFile = os.getenv("PREFIX_FILE")
        runS3(wc)

# Cloud Function entry point
def handler(event, context):
    logging.getLogger().setLevel(logging.INFO)
    sdk = yandexcloud.SDK(user_agent=USER_AGENT)
    mode = os.getenv("MODE")
    runAny(sdk, mode)

# export LBX_SECRET_ID=e6qr6lb9vjs2s19kf1eb
# export SA_KEY_FILE=/Users/mzinal/Magic/key-ydb-sa1.json
# export SA_KEY_FILE=/home/zinal/Keys/ydb-sa1-key1.json
# export DOCAPI_ENDPOINT=https://docapi.serverless.yandexcloud.net/ru-central1/b1gfvslmokutuvt2g019/etngt3b6eh9qfc80vt54
# export TABLE_NAME=delta_log
# python3 cf-cleanup/cfunc.py ddb
# 
# export LBX_SECRET_ID=e6qr6lb9vjs2s19kf1eb
# export PREFIX_FILE=s3://dproc-wh/config/delta-prefixes.txt
# python3 s3
if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    mode = 'ddb'
    if len(sys.argv) > 1:
        mode = sys.argv[1]
    sakey_filename = os.getenv('SA_KEY_FILE')
    if sakey_filename is None:
        sakey_filename = 'sakey.json'
    with open(sakey_filename) as infile:
        sdk = yandexcloud.SDK(service_account_key=json.load(infile), user_agent=USER_AGENT)
    runAny(sdk, mode)
