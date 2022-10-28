import os
import json
import yandex.cloud.dataproc.v1.cluster_pb2 as cluster_pb
import yandex.cloud.dataproc.v1.cluster_service_pb2 as cluster_service_pb
import yandex.cloud.dataproc.v1.cluster_service_pb2_grpc as cluster_service_grpc_pb
import yandexcloud

YC_FOLDER_ID = os.getenv("YC_FOLDER_ID")
USER_AGENT = 'ycloud-python-sdk:dataproc.vm_labeler'

def main():
    with open("../keys/dataproc-binder-key.json") as infile:
        sdk = yandexcloud.SDK(service_account_key=json.load(infile), user_agent=USER_AGENT)
    clusterStub = sdk.client(cluster_service_grpc_pb.ClusterServiceStub)
    listRequest = cluster_service_pb.ListClustersRequest(folder_id=YC_FOLDER_ID)
    listOperation = clusterStub.List(listRequest)
    listResponse = sdk.wait_operation_and_get_result(listOperation, response_type=cluster_service_pb.ListClustersResponse)
    print("Complete!\n")

if __name__ == '__main__':
    main()
