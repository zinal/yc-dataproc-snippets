#! /bin/sh

# Collect the dependencies for Yandex Cloud Java SDK.
# Work in progress, not to be actually used.

rm -rf deps
mkdir deps

GRPC_VER=1.43.2
PROTOBUF_VER=3.19.2
YCSDK_VER=2.5.2

cp -v ~/.m2/repository/io/grpc/grpc-api/${GRPC_VER}/grpc-api-${GRPC_VER}.jar deps/
cp -v ~/.m2/repository/io/grpc/grpc-context/${GRPC_VER}/grpc-context-${GRPC_VER}.jar deps/
cp -v ~/.m2/repository/io/grpc/grpc-core/${GRPC_VER}/grpc-core-${GRPC_VER}.jar deps/
cp -v ~/.m2/repository/io/grpc/grpc-netty-shaded/${GRPC_VER}/grpc-netty-shaded-${GRPC_VER}.jar deps/
cp -v ~/.m2/repository/io/grpc/grpc-protobuf/${GRPC_VER}/grpc-protobuf-${GRPC_VER}.jar deps/
cp -v ~/.m2/repository/io/grpc/grpc-protobuf-lite/${GRPC_VER}/grpc-protobuf-lite-${GRPC_VER}.jar deps/
cp -v ~/.m2/repository/io/grpc/grpc-stub/${GRPC_VER}/grpc-stub-${GRPC_VER}.jar deps/
cp -v ~/.m2/repository/io/perfmark/perfmark-api/0.23.0/perfmark-api-0.23.0.jar deps/
cp -v ~/.m2/repository/com/google/protobuf/protobuf-java/${PROTOBUF_VER}/protobuf-java-${PROTOBUF_VER}.jar deps/
cp -v ~/.m2/repository/com/google/protobuf/protobuf-java-util/${PROTOBUF_VER}/protobuf-java-util-${PROTOBUF_VER}.jar deps/
cp -v ~/.m2/repository/com/yandex/cloud/java-genproto/${YCSDK_VER}/java-genproto-${YCSDK_VER}.jar deps/yc-java-genproto-${YCSDK_VER}.jar
cp -v ~/.m2/repository/com/yandex/cloud/java-sdk-auth/${YCSDK_VER}/java-sdk-auth-${YCSDK_VER}.jar deps/yc-java-sdk-auth-${YCSDK_VER}.jar
cp -v ~/.m2/repository/com/yandex/cloud/java-sdk-services/${YCSDK_VER}/java-sdk-services-${YCSDK_VER}.jar deps/yc-java-sdk-services-${YCSDK_VER}.jar

# End Of File