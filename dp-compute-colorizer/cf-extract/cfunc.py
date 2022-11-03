import os
import json
import logging
import traceback
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import ydb

class WorkContext(object):
    __slots__ = ("pool", "dbpath", "table_prefix")

    def __init__(self, pool: ydb.SessionPool, dbpath: str, table_prefix: str) -> None:
        self.pool = pool
        self.dbpath = dbpath
        self.table_prefix = table_prefix

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

def runExtract(ctx: WorkContext, dt: int, upd_tv: int):
    logging.debug("*** Extraction for date {}".format(dt))
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
            logging.debug("{} {} {} {}".format(cur_obj_id, row.vm_id, row.cluster_id, row.otype))
        return True
    while True:
        if not ctx.pool.retry_operation_sync(callee):
            break
    # Save the current update timestamp
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

def runCtx(ctx: WorkContext):
    if not tableExists(ctx, "item_ref"):
        raise Exception("YDB table does not exist: item_ref")
    if not tableExists(ctx, "item_xtr"):
        raise Exception("YDB table does not exist: item_xtr")
    # YC Billing treats dates in Moscow time
    xtv = datetime.now(ZoneInfo('Europe/Moscow'))
    xcur = xtv.date()
    xprev = (xtv - timedelta(days=1)).date()
    # Integer "dates" in YYYYMMDD format
    date_now = (xcur.year * 10000) + (xcur.month * 100) + xcur.day
    date_before = (xprev.year * 10000) + (xprev.month * 100) + xprev.day
    # Check + extract for day before
    stamp_prev, stamp_cur = readUpdateLabels(ctx, date_before)
    if stamp_prev < stamp_cur:
        runExtract(ctx, date_before, stamp_cur)
    # Check + extract for today
    stamp_prev, stamp_cur = readUpdateLabels(ctx, date_now)
    if stamp_prev < stamp_cur:
        runExtract(ctx, date_now, stamp_cur)

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
