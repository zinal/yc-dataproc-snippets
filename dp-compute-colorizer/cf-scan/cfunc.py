import os
import json
import logging
import traceback
import time
from datetime import datetime
from zoneinfo import ZoneInfo
import yandex.cloud.dataproc.v1.cluster_pb2 as cluster_pb
import yandex.cloud.dataproc.v1.cluster_service_pb2 as cluster_service_pb
import yandex.cloud.dataproc.v1.cluster_service_pb2_grpc as cluster_service_grpc_pb
import yandex.cloud.dataproc.v1.subcluster_service_pb2 as subcluster_service_pb
import yandex.cloud.dataproc.v1.subcluster_service_pb2_grpc as subcluster_service_grpc_pb
import yandex.cloud.compute.v1.instancegroup.instance_group_service_pb2 as instance_group_service_pb
import yandex.cloud.compute.v1.instancegroup.instance_group_service_pb2_grpc as instance_group_service_grpc_pb
import yandex.cloud.compute.v1.instance_service_pb2 as instance_service_pb;
import yandex.cloud.compute.v1.instance_service_pb2_grpc as instance_service_grpc_pb;
import yandexcloud
import ydb

USER_AGENT = 'ycloud-python-sdk:dataproc.compute_colorizer'
PAGE_SIZE = 100
MAX_RECORDS = 1000

class ItemRecord(object):
    __slots__ = ("crdate", "obj_id", "vm_id", "cluster_id", "otype", "upd_tv")

    def __init__(self, upd_tv: int, crdate: int, obj_id: str, vm_id: str, cluster_id: str, otype: str) -> None:
        self.obj_id = obj_id
        self.upd_tv = upd_tv
        self.crdate = crdate
        self.vm_id = vm_id
        self.cluster_id = cluster_id
        self.otype = otype

class WorkContext(object):
    __slots__ = ("sdk", "pool", 
                "folder_id", "dbpath", "table_prefix", 
                "cur_date", "cur_items", "cur_tv")

    def __init__(self,
        sdk: yandexcloud.SDK, pool: ydb.SessionPool, 
        folder_id: str, dbpath: str, table_prefix: str
    ) -> None:
        self.sdk = sdk
        self.pool = pool
        self.folder_id = folder_id
        self.dbpath = dbpath
        self.table_prefix = table_prefix
        # YC Billing treats dates in Moscow time
        xtv = datetime.now(ZoneInfo('Europe/Moscow'))
        xdate = xtv.date()
        # Integer "dates" in YYYYMMDD format
        self.cur_date = (xdate.year * 10000) + (xdate.month * 100) + xdate.day
        self.cur_tv = int(time.time() * 10000000)
        self.cur_items = list()

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

def createTables(ctx: WorkContext):
    def callee(session: ydb.Session):
        if not tableExistsSess(ctx, session, "item_ref"):
            logging.info("Creating table {}/item_ref".format(ctx.table_prefix))
            session.execute_scheme(
                """
                CREATE TABLE `{}/item_ref`(
                    obj_id Utf8,
                    crdate Int32,
                    vm_id Utf8,
                    cluster_id Utf8,
                    otype Utf8,
                    upd_tv Int64,
                    sync_tv Int64,
                    PRIMARY KEY(crdate, obj_id)
                )
                """.format(ctx.table_prefix)
            )
    return ctx.pool.retry_operation_sync(callee)

def saveItems(ctx: WorkContext, items: list):
    def callee(session: ydb.Session):
        query = """
        DECLARE $input AS List<Struct<
            obj_id: Utf8,
            crdate: Int32,
            vm_id: Utf8,
            cluster_id: Utf8,
            otype: Utf8,
            upd_tv: Int64>>;
        UPSERT INTO `{}/item_ref` SELECT * FROM AS_TABLE($input);
        """.format(ctx.table_prefix)
        qp = session.prepare(query)
        session.transaction(ydb.SerializableReadWrite()).execute(
            qp, { "$input": items }, commit_tx=True,
        )
    if len(items) > 0:
        retval = ctx.pool.retry_operation_sync(callee)
        items.clear()
        return retval
    return None

def appendItem(ctx: WorkContext, item: ItemRecord):
    if item is not None:
        ctx.cur_items.append(item)
    if len(ctx.cur_items) >= MAX_RECORDS or item is None:
        saveItems(ctx, ctx.cur_items)

def appendVm(ctx: WorkContext, clusterId: str, instanceId: str):
    appendItem(ctx, ItemRecord(ctx.cur_tv, ctx.cur_date, instanceId, instanceId, clusterId, "vm"))

def appendDisk(ctx: WorkContext, clusterId: str, instanceId: str, diskId: str):
    appendItem(ctx, ItemRecord(ctx.cur_tv, ctx.cur_date, diskId, instanceId, clusterId, "disk"))

def appendNfs(ctx: WorkContext, clusterId: str, instanceId: str, nfsId: str):
    appendItem(ctx, ItemRecord(ctx.cur_tv, ctx.cur_date, nfsId, instanceId, clusterId, "nfs"))

def processHost(ctx: WorkContext, clusterId, instanceId):
    logging.debug('Processing host {} for cluster {}'.format(instanceId, clusterId))
    appendVm(ctx, clusterId, instanceId)
    try:
        instService = ctx.sdk.client(instance_service_grpc_pb.InstanceServiceStub)
        reqGet = instance_service_pb.GetInstanceRequest(instance_id=instanceId)
        respGet = instService.Get(reqGet)
    except Exception as e:
        logging.error(traceback.format_exc())
    if respGet is not None:
        if respGet.boot_disk is not None:
            appendDisk(ctx, clusterId, instanceId, respGet.boot_disk.disk_id)
        if respGet.secondary_disks is not None:
            for d in respGet.secondary_disks:
                appendDisk(ctx, clusterId, instanceId, d.disk_id)
        if respGet.filesystems is not None:
            for f in respGet.filesystems:
                appendNfs(ctx, clusterId, instanceId, f.filesystem_id)

def processCluster(ctx: WorkContext, clusterService, cluster):
    logging.debug('Processing cluster {}'.format(cluster.id))
    pageToken = None
    while True:
        req = cluster_service_pb.ListClusterHostsRequest(
            cluster_id=cluster.id, page_size=PAGE_SIZE, page_token=pageToken)
        resp = clusterService.ListHosts(req)
        for host in resp.hosts:
            processHost(ctx, cluster.id, host.compute_instance_id)
        pageToken = resp.next_page_token
        if len(resp.hosts) < PAGE_SIZE:
            break

def runCtx(ctx: WorkContext):
    createTables(ctx)
    clusterService = ctx.sdk.client(cluster_service_grpc_pb.ClusterServiceStub)
    pageToken = None
    while True:
        req = cluster_service_pb.ListClustersRequest(
            folder_id=yc_folder_id, page_size=PAGE_SIZE, page_token=pageToken)
        resp = clusterService.List(req)
        for cluster in resp.clusters:
            processCluster(ctx, clusterService, cluster)
        pageToken = resp.next_page_token
        if len(resp.clusters) < PAGE_SIZE:
            break
    # Flush the remaining records to YDB table
    appendItem(ctx, None)

def run(sdk: yandexcloud.SDK, yc_folder_id: str):
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
            runCtx(WorkContext(sdk, pool, yc_folder_id, ydb_database, ydb_path))

def handler(event, context):
    logging.getLogger().setLevel(logging.INFO)
    logging.getLogger('ydb').setLevel(logging.WARNING)
    yc_folder_id = os.getenv("YC_FOLDER_ID")
    if yc_folder_id is None or len(yc_folder_id)==0:
        yc_folder_id = event["messages"][0]["event_metadata"]["folder_id"]
    sdk = yandexcloud.SDK(user_agent=USER_AGENT)
    run(sdk, yc_folder_id)

# export YC_PROFILE=`yc config profile list | grep -E 'ACTIVE$' | (read x y && echo $x)`
# export YC_FOLDER_ID=`yc config profile get ${YC_PROFILE} | grep -E '^folder-id: ' | (read x y && echo $y)`
# export YDB_PATH=billing1
# export YDB_ENDPOINT=grpcs://ydb.serverless.yandexcloud.net:2135
# export YDB_DATABASE=/ru-central1/b1g1hfek2luako6vouqb/etno6m1l1lf4ae3j01ej
# export YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS=keys/dp-compute-colorizer.json
# python3 cf/cfunc.py
if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    logging.getLogger('ydb').setLevel(logging.WARNING)
    yc_folder_id = os.getenv("YC_FOLDER_ID")
    yc_sa_key_filename = os.getenv("YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS")
    with open(yc_sa_key_filename) as infile:
        sdk = yandexcloud.SDK(service_account_key=json.load(infile), user_agent=USER_AGENT)
    run(sdk, yc_folder_id)
