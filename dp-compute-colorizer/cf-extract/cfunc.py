import os
import io
import sys
import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import boto3
import yandexcloud
import ydb
from yandex.cloud.lockbox.v1.payload_service_pb2 import GetPayloadRequest
from yandex.cloud.lockbox.v1.payload_service_pb2_grpc import PayloadServiceStub

class WorkContext(object):
    __slots__ = ("pool", "dbpath", "table_prefix", "s3_prefix", "s3_bucket")

    def __init__(self, pool: ydb.SessionPool, 
            dbpath: str, table_prefix: str, 
            s3_bucket: str, s3_prefix: str) -> None:
        self.pool = pool
        self.dbpath = dbpath
        self.table_prefix = table_prefix
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix

fixed_date = None
boto_session = None
storage_client = None

def getBotoSession():
    global boto_session
    if boto_session is not None:
        return boto_session

    access_key = os.getenv('ACCESS_KEY_ID')
    secret_key = os.getenv('SECRET_ACCESS_KEY')

    if access_key is None or secret_key is None:
        # initialize lockbox and read secret value
        yc_sdk = yandexcloud.SDK()
        channel = yc_sdk._channels.channel("lockbox-payload")
        lockbox = PayloadServiceStub(channel)
        response = lockbox.Get(GetPayloadRequest(secret_id=os.environ['SECRET_ID']))
        # extract values from secret
        access_key = None
        secret_key = None
        for entry in response.entries:
            if entry.key == 'ACCESS_KEY_ID':
                access_key = entry.text_value
            elif entry.key == 'SECRET_ACCESS_KEY':
                secret_key = entry.text_value

    if access_key is None or secret_key is None:
        raise Exception("secrets required")

    # initialize boto session
    boto_session = boto3.session.Session(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )
    return boto_session

def getS3Client():
    global storage_client
    if storage_client is not None:
        return storage_client
    storage_client = getBotoSession().client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net',
        region_name='ru-central1'
    )
    return storage_client

def ydbDirExists(driver, path):
    try:
        return driver.scheme_client.describe_path(path).is_directory()
    except ydb.SchemeError:
        return False

def tableExistsSess(ctx: WorkContext, session: ydb.Session, tableName: str) -> bool:
    try:
        result = session.describe_table(ctx.dbpath + "/" + ctx.table_prefix + "/" + tableName)
        return len(result.columns) > 0
    except ydb.SchemeError:
        return False

def tableExists(ctx: WorkContext, tableName: str) -> bool:
    def callee(session: ydb.Session):
        return tableExistsSess(ctx, session, tableName)
    return ctx.pool.retry_operation_sync(callee)

def readUpdateLabels(ctx: WorkContext, dt: int):
    def callee(session: ydb.Session):
        qtext = """
            DECLARE $crdate AS Int32;
            SELECT upd_tv FROM `{}/item_xtr` WHERE crdate=$crdate;
        """.format(ctx.table_prefix)
        qp = session.prepare(qtext)
        rs = session.transaction().execute(qp, {"$crdate" : dt})
        if (rs is None or len(rs)==0 or rs[0].rows is None or len(rs[0].rows)==0):
            stamp_prev = 0
        else:
            stamp_prev = rs[0].rows[0].upd_tv
        if stamp_prev is None:
            stamp_prev = 0
        qtext = """
            DECLARE $crdate AS Int32;
            SELECT MAX(upd_tv) AS upd_tv FROM `{}/item_ref`
            WHERE crdate=$crdate;
        """.format(ctx.table_prefix)
        qp = session.prepare(qtext)
        rs = session.transaction().execute(qp, {"$crdate" : dt})
        if (rs is None or len(rs)==0 or rs[0].rows is None or len(rs[0].rows)==0):
            stamp_cur = 0
        else:
            stamp_cur = rs[0].rows[0].upd_tv
        if stamp_cur is None:
            stamp_cur = 0
        return stamp_prev, stamp_cur
    return ctx.pool.retry_operation_sync(callee)

def formatDate(dt: int) -> str:
    dt_year = dt // 10000
    dt_month = (dt - dt_year*10000) // 100
    dt_day = dt - dt_year*10000 - dt_month*100
    return str(dt_year) + "-" + str(dt_month).zfill(2) + "-" + str(dt_day).zfill(2)

# Extract the dictionary data to the temporary file
def runExtract(ctx: WorkContext, dt: int, fout: io.TextIOWrapper):
    logging.debug("*** Extraction for date {}".format(dt))
    dtstr = formatDate(dt)
    qtext = """
        DECLARE $crdate AS Int32;
        DECLARE $obj_id AS Utf8;
        DECLARE $limit AS Int32;
        SELECT obj_id, vm_id, cluster_id, otype FROM `{}/item_ref`
        WHERE crdate=$crdate AND obj_id > $obj_id
        ORDER BY obj_id
        LIMIT $limit;
    """.format(ctx.table_prefix)
    cur_obj_id = ''
    def callee(session: ydb.Session):
        nonlocal cur_obj_id
        logging.debug("*** Iteration for date {} obj_id {}".format(dt, cur_obj_id))
        qp = session.prepare(qtext)
        rs = session.transaction().execute(
            qp, 
            {
                "$crdate" : dt, 
                "$obj_id": cur_obj_id,
                "$limit": 100
            }
        )
        if not rs[0].rows:
            return False
        for row in rs[0].rows:
            cur_obj_id = row.obj_id
            ln = "{},{},{},{},{}\n".format(dtstr, cur_obj_id, row.otype, row.vm_id, row.cluster_id)
            fout.write(ln)
        return True
    while True:
        if not ctx.pool.retry_operation_sync(callee):
            break

# Save the current update timestamp
def updateCurrentStamp(ctx: WorkContext, dt: int, upd_tv: int):
    qtext = """
        DECLARE $crdate AS Int32;
        DECLARE $upd_tv AS Int64;
        REPLACE INTO `{}/item_xtr`(crdate, upd_tv) VALUES($crdate, $upd_tv);
    """.format(ctx.table_prefix)
    def callee(session: ydb.Session):
        qp = session.prepare(qtext)
        session.transaction(ydb.SerializableReadWrite()).execute(
            qp, 
            { 
                "$crdate": dt,
                "$upd_tv": upd_tv,
            },
            commit_tx=True, )
    ctx.pool.retry_operation_sync(callee)

# Extract the data for the day specified, and upload it to the S3 bucket
def extractDay(ctx: WorkContext, dt: int):
    stamp_prev, stamp_cur = readUpdateLabels(ctx, dt)
    if stamp_prev < stamp_cur:
        logging.info("Refreshing S3 data for {} at path {}/{}".format(dt, ctx.s3_bucket, ctx.s3_prefix))
        fname = "/tmp/dp-compute-colorizer-data.txt"
        with open(fname, "wt") as fout:
            fout.write("dt,obj_id,obj_type,vm_id,cluster_id\n")
            runExtract(ctx, dt, fout)
        outname = ctx.s3_prefix + "/" + str(dt) + ".csv"
        getS3Client().upload_file(fname, ctx.s3_bucket, outname)
        updateCurrentStamp(ctx, dt, stamp_cur)
        return True
    else:
        logging.debug("Skipping S3 refresh for {}".format(dt))
    return False

def runCtx(ctx: WorkContext):
    if not tableExists(ctx, "item_ref"):
        raise Exception("YDB table does not exist: item_ref")
    if not tableExists(ctx, "item_xtr"):
        raise Exception("YDB table does not exist: item_xtr")
    if fixed_date is None:
        # YC Billing treats dates in Moscow time
        xtv = datetime.now(ZoneInfo('Europe/Moscow'))
        xcur = xtv.date()
        xprev = (xtv - timedelta(days=1)).date()
        # Integer "dates" in YYYYMMDD format
        date_now = (xcur.year * 10000) + (xcur.month * 100) + xcur.day
        date_before = (xprev.year * 10000) + (xprev.month * 100) + xprev.day
        # Check + extract for day before
        extractDay(ctx, date_before)
        # Check + extract for today
        extractDay(ctx, date_now)
    else:
        extractDay(ctx, fixed_date)

def run():
    ydb_endpoint = os.getenv("YDB_ENDPOINT")
    if ydb_endpoint is None or len(ydb_endpoint)==0:
        raise Exception("missing YDB_ENDPOINT env")
    ydb_database = os.getenv("YDB_DATABASE")
    if ydb_database is None or len(ydb_database)==0:
        raise Exception("missing YDB_DATABASE env")
    ydb_path = os.getenv("YDB_PATH")
    if ydb_path is None or len(ydb_path)==0:
        ydb_path = "dp-compute-colorizer"
    s3_bucket = os.getenv("S3_BUCKET")
    if s3_bucket is None or len(s3_bucket)==0:
        raise Exception("missing S3_BUCKET env")
    s3_prefix = os.getenv("S3_PREFIX")
    if s3_prefix is None or len(s3_prefix)==0:
        s3_prefix = "dp-compute-colorizer"
    with ydb.Driver(endpoint=ydb_endpoint, database=ydb_database) as driver:
        driver.wait(timeout=5, fail_fast=True)
        if not ydbDirExists(driver, ydb_database + "/" + ydb_path):
            raise Exception("Target YDB directory does not exist", ydb_path)
        with ydb.SessionPool(driver) as pool:
            runCtx(WorkContext(pool, ydb_database, ydb_path, s3_bucket, s3_prefix))

def handler(event, context):
    logging.getLogger().setLevel(logging.INFO)
    logging.getLogger('ydb').setLevel(logging.WARNING)
    run()

# export ACCESS_KEY_ID=...
# export SECRET_ACCESS_KEY=...
# export S3_BUCKET=billing1
# export S3_PREFIX=dp-compute-colorizer
# export YDB_PATH=billing1
# export YDB_ENDPOINT=grpcs://ydb.serverless.yandexcloud.net:2135
# export YDB_DATABASE=/ru-central1/b1g1hfek2luako6vouqb/etno6m1l1lf4ae3j01ej
# export YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS=keys/dp-compute-colorizer.json
# python3 cf-extract/cfunc.py
if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    logging.getLogger('ydb').setLevel(logging.WARNING)
    logging.getLogger('s3transfer').setLevel(logging.INFO)
    logging.getLogger('botocore').setLevel(logging.INFO)
    logging.getLogger('urllib3').setLevel(logging.INFO)
    if len(sys.argv) > 1:
        fixed_date = int(sys.argv[1])
    run()
