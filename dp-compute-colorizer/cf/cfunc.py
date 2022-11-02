import os
import json
import logging
import traceback
from datetime import date, datetime
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
import google.protobuf.field_mask_pb2 as field_mask_pb;
import yandexcloud
import ydb

USER_AGENT = 'ycloud-python-sdk:dataproc.compute_colorizer'
PAGE_SIZE = 100
MAX_RECORDS = 1000

class ItemRecord(object):
    __slots__ = ("obj_id", "vm_id", "cluster_id", "crdate", "otype")

    def __init__(self, obj_id: str, vm_id: str, cluster_id: str, crdate: date, otype: str) -> None:
        self.obj_id = obj_id
        self.vm_id = vm_id
        self.cluster_id = cluster_id
        self.crtime = crdate
        self.otype = otype

class WorkContext(object):
    __slots__ = ("sdk", "pool", 
                "folder_id", "dbpath", "table_prefix", 
                "cur_date", "cur_items")

    def __init__(self,
        sdk: yandexcloud.SDK, pool: ydb.SessionPool, 
        folder_id: str, dbpath: str, table_prefix: str
    ) -> None:
        self.sdk = sdk
        self.pool = pool
        self.folder_id = folder_id
        self.dbpath = dbpath
        self.table_prefix = table_prefix
        self.cur_date = datetime.now(ZoneInfo('Europe/Moscow')).date()
        self.cur_items = list()

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
            session.execute_scheme(
                """
                CREATE TABLE `{}/item_ref`(
                    obj_id String,
                    crdate Date,
                    vm_id String,
                    cluster_id String,
                    otype String,
                    PRIMARY KEY(obj_id, crdate)
                )
                """.format(ctx.table_prefix)
            )
    return ctx.pool.retry_operation_sync(callee)

def saveItems(ctx: WorkContext, items: list):
    def callee(session: ydb.Session):
        query = """
        DECLARE $input AS List<Struct<
            obj_id: String,
            crdate: Date,
            vm_id: String,
            cluster_id: String,
            otype: String>>;
        REPLACE INTO `{}/item_ref` SELECT * FROM AS_TABLE($input);
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
    appendItem(ctx, ItemRecord(instanceId, instanceId, clusterId, ctx.cur_date, "vm"))

def appendDisk(ctx: WorkContext, clusterId: str, instanceId: str, diskId: str):
    appendItem(ctx, ItemRecord(diskId, instanceId, clusterId, ctx.cur_date, "disk"))

def appendNfs(ctx: WorkContext, clusterId: str, instanceId: str, nfsId: str):
    appendItem(ctx, ItemRecord(nfsId, instanceId, clusterId, ctx.cur_date, "nfs"))

def processInstance(ctx: WorkContext, clusterId: str, instanceId: str):
    logging.debug('Processing instance {} for cluster {}'.format(instanceId, clusterId))
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

def processInstanceGroup(ctx: WorkContext, clusterId: str, groupId: str):
    logging.debug('Processing instance group {} for cluster {}'.format(groupId, clusterId))
    igService = ctx.sdk.client(instance_group_service_grpc_pb.InstanceGroupServiceStub)
    pageToken = None
    while True:
        req = instance_group_service_pb.ListInstanceGroupInstancesRequest(
            instance_group_id=groupId, page_size=PAGE_SIZE, page_token=pageToken)
        resp = igService.ListInstances(req)
        for instance in resp.instances:
            processInstance(ctx, clusterId, instance.instance_id)
        pageToken = resp.next_page_token
        if len(resp.instances) < PAGE_SIZE:
            break

def processSubcluster(ctx: WorkContext, subcluster):
    if subcluster.instance_group_id == None or len(subcluster.instance_group_id) == 0:
        logging.debug('Skipping subcluster {}'.format(subcluster.id))
    else:
        processInstanceGroup(ctx, subcluster.cluster_id, subcluster.instance_group_id)

def processCluster(ctx: WorkContext, cluster):
    logging.debug('Processing cluster {}'.format(cluster.id))
    subclusterService = ctx.sdk.client(subcluster_service_grpc_pb.SubclusterServiceStub)
    pageToken = None
    while True:
        req = subcluster_service_pb.ListSubclustersRequest(
            cluster_id=cluster.id, page_size=PAGE_SIZE, page_token=pageToken)
        resp = subclusterService.List(req)
        for subcluster in resp.subclusters:
            processSubcluster(ctx, subcluster)
        pageToken = resp.next_page_token
        if len(resp.subclusters) < PAGE_SIZE:
            break

def run(ctx: WorkContext, yc_folder_id: str):
    clusterService = ctx.sdk.client(cluster_service_grpc_pb.ClusterServiceStub)
    pageToken = None
    while True:
        req = cluster_service_pb.ListClustersRequest(
            folder_id=yc_folder_id, page_size=PAGE_SIZE, page_token=pageToken)
        resp = clusterService.List(req)
        for cluster in resp.clusters:
            processCluster(ctx, cluster)
        pageToken = resp.next_page_token
        if len(resp.clusters) < PAGE_SIZE:
            break

def handler(event, context):
    logging.getLogger().setLevel(logging.INFO)
    sdk = yandexcloud.SDK(user_agent=USER_AGENT)
    yc_folder_id = event["messages"][0]["event_metadata"]["folder_id"]
    run(sdk, yc_folder_id)

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    with open("dp-compute-colorizer-key.json") as infile:
        sdk = yandexcloud.SDK(service_account_key=json.load(infile), user_agent=USER_AGENT)
    yc_folder_id = os.getenv("YC_FOLDER_ID")
    run(sdk, yc_folder_id)
