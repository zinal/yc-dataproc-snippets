import os
import json
import logging
import yandex.cloud.dataproc.v1.cluster_pb2 as cluster_pb
import yandex.cloud.dataproc.v1.cluster_service_pb2 as cluster_service_pb
import yandex.cloud.dataproc.v1.cluster_service_pb2_grpc as cluster_service_grpc_pb
import yandex.cloud.dataproc.v1.subcluster_service_pb2 as subcluster_service_pb
import yandex.cloud.dataproc.v1.subcluster_service_pb2_grpc as subcluster_service_grpc_pb
import yandex.cloud.compute.v1.instancegroup.instance_group_service_pb2 as instance_group_service_pb
import yandex.cloud.compute.v1.instancegroup.instance_group_service_pb2_grpc as instance_group_service_grpc_pb
import yandexcloud

YC_FOLDER_ID = os.getenv("YC_FOLDER_ID")
USER_AGENT = 'ycloud-python-sdk:dataproc.compute_colorizer'
PAGE_SIZE = 100

def processInstance(sdk, clusterId, instanceId):
    logging.info('Processing instance {} for cluster {}'.format(instanceId, clusterId))

def processInstanceGroup(sdk, clusterId, groupId):
    logging.info('Processing instance group {} for cluster {}'.format(groupId, clusterId))
    igService = sdk.client(instance_group_service_grpc_pb.InstanceGroupServiceStub)
    pageToken = None
    while True:
        req = instance_group_service_pb.ListInstanceGroupInstancesRequest(
            instance_group_id=groupId, page_size=PAGE_SIZE, page_token=pageToken)
        resp = igService.ListInstances(req)
        for instance in resp.instances:
            processInstance(sdk, clusterId, instance.instance_id)
        pageToken = resp.next_page_token
        if len(resp.instances) < PAGE_SIZE:
            break

def processSubcluster(sdk, subcluster):
    if subcluster.instance_group_id == None or len(subcluster.instance_group_id) == 0:
        logging.info('Skipping subcluster {}'.format(subcluster.id))
    else:
        processInstanceGroup(sdk, subcluster.cluster_id, subcluster.instance_group_id)

def processCluster(sdk, cluster):
    logging.info('Processing cluster {}'.format(cluster.id))
    subclusterService = sdk.client(subcluster_service_grpc_pb.SubclusterServiceStub)
    pageToken = None
    while True:
        req = subcluster_service_pb.ListSubclustersRequest(
            cluster_id=cluster.id, page_size=PAGE_SIZE, page_token=pageToken)
        resp = subclusterService.List(req)
        for subcluster in resp.subclusters:
            processSubcluster(sdk, subcluster)
        pageToken = resp.next_page_token
        if len(resp.subclusters) < PAGE_SIZE:
            break

def run(sdk):
    clusterService = sdk.client(cluster_service_grpc_pb.ClusterServiceStub)
    pageToken = None
    while True:
        req = cluster_service_pb.ListClustersRequest(
            folder_id=YC_FOLDER_ID, page_size=PAGE_SIZE, page_token=pageToken)
        resp = clusterService.List(req)
        for cluster in resp.clusters:
            processCluster(sdk, cluster)
        pageToken = resp.next_page_token
        if len(resp.clusters) < PAGE_SIZE:
            break

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    with open("../keys/dataproc-binder-key.json") as infile:
        sdk = yandexcloud.SDK(service_account_key=json.load(infile), user_agent=USER_AGENT)
    run(sdk)
