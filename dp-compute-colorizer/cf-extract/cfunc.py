import os
import json
import logging
import traceback
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import ydb

class WorkContext(object):
    __slots__ = ("pool", 
                "dbpath", "table_prefix",
                "inst_service",
                "cur_date", "prev_date", "cur_tv")

    def __init__(self, pool: ydb.SessionPool, dbpath: str, table_prefix: str) -> None:
        self.pool = pool
        self.dbpath = dbpath
        self.table_prefix = table_prefix
        # YC Billing treats dates in Moscow time
        xtv = datetime.now(ZoneInfo('Europe/Moscow'))
        xdate1 = xtv.date()
        xdate2 = (xtv - timedelta(days=1)).date()
        # Integer "dates" in YYYYMMDD format
        self.cur_date = (xdate1.year * 10000) + (xdate1.month * 100) + xdate1.day
        self.prev_date = (xdate2.year * 10000) + (xdate2.month * 100) + xdate2.day
        self.cur_tv = int(time.time() * 10000000)

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

def runCtx(ctx: WorkContext):
    if not tableExists(ctx, "item_ref"):
        raise Exception("YDB table does not exist: item_ref")
    logging.debug("Extraction on {} and {} started".format(ctx.cur_date, ctx.prev_date))

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
    with ydb.Driver(endpoint=ydb_endpoint, database=ydb_database) as driver:
        driver.wait(timeout=5, fail_fast=True)
        if not ydbDirExists(driver, ydb_database + "/" + ydb_path):
            raise Exception("Target YDB directory does not exist", ydb_path)
        with ydb.SessionPool(driver) as pool:
            runCtx(WorkContext(pool, ydb_database, ydb_path))

def handler(event, context):
    logging.getLogger().setLevel(logging.INFO)
    logging.getLogger('ydb').setLevel(logging.WARNING)
    run()

# export YDB_PATH=billing1
# export YDB_ENDPOINT=grpcs://ydb.serverless.yandexcloud.net:2135
# export YDB_DATABASE=/ru-central1/b1g1hfek2luako6vouqb/etno6m1l1lf4ae3j01ej
# export YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS=keys/dp-compute-colorizer.json
# python3 cf-extract/cfunc.py
if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    logging.getLogger('ydb').setLevel(logging.WARNING)
    run()
