import os
import sys
import json
import logging
import time
from enum import IntEnum
from urllib.parse import urlparse
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
                 "s3session", "s3temp",
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

class S3ItemMode(IntEnum):
    WAREHOUSE = 2
    DATABASE = 1
    TABLE = 0

def S3ItemMode_conv(mode: str):
    if mode=="W":
        return S3ItemMode.WAREHOUSE
    if mode=="D":
        return S3ItemMode.DATABASE
    if mode=="T":
        return S3ItemMode.TABLE
    raise Exception(f"Unsupported S3 processing mode: {mode}")

def removeS3extras(wc: WorkContext, bucket: str):
    for tabkey, tabval in wc.s3temp.items():
        if len(tabval) > 1:
            tabval.sort()
            for ix in range(0, len(tabval)-1):
                key = tabval[ix][1]
                logging.info(f"Removing {key}")
                wc.s3session.delete_object(Bucket=bucket, Key=key)

def processS3_item(wc: WorkContext, mode: S3ItemMode, bucket: str, prefix: str, key: str):
    sval = key
    if sval.startswith(prefix):
        sval = sval[len(prefix):] # cut the prefix
    aval = sval.split(sep='/')
    if len(aval) != (3 + int(mode)):
        return False
    ix = int(mode)
    if aval[ix]!='_delta_log' or aval[ix+1]!='.tmp':
        return False
    fname = aval[ix+2]
    if fname.find('.json.') < 0:
        return False
    tabkey = ''
    for i in range(0, ix):
        tabkey = tabkey + '/' + aval[i]
    tabval = wc.s3temp.get(tabkey, None)
    if tabval is None:
        tabval = []
        wc.s3temp[tabkey] = tabval
    tabval.append([fname, key])
    return True

def processS3(wc: WorkContext, bucket: str, mode: S3ItemMode, prefix: str):
    if not prefix.endswith("/"):
        prefix = prefix + "/"
    if prefix.startswith("/"):
        prefix = prefix[1:]
    logging.info(f"Processing prefix '{prefix}' in bucket '{bucket}' as {mode}")
    # Dict structure below:
    # "database.db/table" -> ["000.json.XYZ", "wh/database.db/table/_delta_log/.tmp/000.json.XYZ"]
    wc.s3temp = dict()
    prevToken = None
    while True:
        if prevToken is None:
            response = wc.s3session.list_objects_v2(Bucket=bucket, Prefix=prefix)
        else:
            response = wc.s3session.list_objects_v2(Bucket=bucket, Prefix=prefix, ContinuationToken=prevToken)
        contents = response.get('Contents', [])
        for item in contents:
            key = item.get('Key', None)
            if key is not None and not key.endswith("/"):
                processS3_item(wc, mode, bucket, prefix, key)
        isTruncated = response.get('IsTruncated', False)
        if not isTruncated:
            break
        prevToken = response.get('NextContinuationToken', None)
    removeS3extras(wc, bucket)

def runS3(wc: WorkContext):
    session = boto3.session.Session()
    wc.s3session = session.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net',
        region_name = wc.awsRegionId,
        aws_access_key_id = wc.awsKeyId,
        aws_secret_access_key = wc.awsKeySecret,
    )
    s3pf = urlparse(wc.s3PrefixFile)
    s3pf_bucket = s3pf.hostname
    s3pf_path = s3pf.path
    if s3pf_path.startswith("/"):
        s3pf_path = s3pf_path[1:]
    logging.info(f"Reading S3 cleanup config from {s3pf_bucket}:{s3pf_path}...")
    config_response = wc.s3session.get_object(Bucket=s3pf_bucket, Key=s3pf_path)
    for line in config_response['Body'].iter_lines():
        line = line.decode('utf-8').strip()
        if not line.startswith("#"):
            items = line.split()
            if len(items)!=3:
                logging.warn(f"Illegal configuration line skipped: {line}")
            else:
                bucket = items[0]
                mode = S3ItemMode_conv(items[1])
                prefix = items[2]
                if len(bucket)>0 and len(prefix)>0:
                    processS3(wc, bucket, mode, prefix)
                else:
                    logging.warn(f"Unparsable configuration line skipped: {line}")

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
    logging.debug(f"Using AWS key id {wc.awsKeyId}")
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

# export SA_KEY_FILE=/Users/mzinal/Magic/key-ydb-sa1.json
# export SA_KEY_FILE=/home/zinal/Keys/ydb-sa1-key1.json
#
# export LBX_SECRET_ID=e6qr6lb9vjs2s19kf1eb
# export DOCAPI_ENDPOINT=https://docapi.serverless.yandexcloud.net/ru-central1/b1gfvslmokutuvt2g019/etngt3b6eh9qfc80vt54
# export TABLE_NAME=delta_log
# python3 cf-cleanup/cfunc.py ddb
# 
# export LBX_SECRET_ID=e6qr6lb9vjs2s19kf1eb
# export PREFIX_FILE=s3://dproc-wh/config/delta-prefixes.txt
# python3 s3
if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    logging.getLogger('s3transfer').setLevel(logging.INFO)
    logging.getLogger('botocore').setLevel(logging.INFO)
    logging.getLogger('urllib3').setLevel(logging.INFO)
    mode = 's3'
    if len(sys.argv) > 1:
        mode = sys.argv[1]
    sakey_filename = os.getenv('SA_KEY_FILE')
    if sakey_filename is None:
        sakey_filename = 'sakey.json'
    with open(sakey_filename) as infile:
        sdk = yandexcloud.SDK(service_account_key=json.load(infile), user_agent=USER_AGENT)
    runAny(sdk, mode)
