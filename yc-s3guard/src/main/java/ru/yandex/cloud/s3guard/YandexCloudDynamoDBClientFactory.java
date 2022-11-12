package ru.yandex.cloud.s3guard;

import java.io.IOException;
import org.apache.commons.lang3.StringUtils;
import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.s3a.s3guard.DynamoDBClientFactory;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.s3a.S3AUtils;
import static org.apache.hadoop.fs.s3a.Constants.S3GUARD_DDB_REGION_KEY;

/**
 * This class works generally like DynamoDBClientFactory.DefaultDynamoDBClientFactory,
 * but differs in two important ways:
 *   (a) it supports the additional configuraiton property "fs.s3a.s3guard.ddb.endpoint"
 *       to specify the DynamoDB endpoint to be used;
 *   (b) it does not check the validity of the AWS region specified.
 * The modified class allows YDB Serverless to be used to serve DynamoDB requests of S3Guard.
 * It has to be added to the Hadoop jars, and configured by setting the property:
 *   fs.s3a.s3guard.ddb.client.factory.impl = ru.yandex.cloud.s3guard.YandexCloudDynamoDBClientFactory
 * @author zinal
 */
public class YandexCloudDynamoDBClientFactory extends Configured implements DynamoDBClientFactory {

    private static final org.slf4j.Logger LOG =
            org.slf4j.LoggerFactory.getLogger(YandexCloudDynamoDBClientFactory.class);

    public static final String S3GUARD_DDB_ENDPOINT_KEY = "fs.s3a.s3guard.ddb.endpoint";

    @Override
    public AmazonDynamoDB createDynamoDBClient(String defaultRegion,
            String bucket, AWSCredentialsProvider credentials) throws IOException {
        Preconditions.checkNotNull(getConf(),
                "Should have been configured before usage");

        final Configuration conf = getConf();
        final ClientConfiguration awsConf = S3AUtils.createAwsConf(conf, bucket);

        final String region = getRegion(conf, defaultRegion);
        final String endpoint = getEndpoint(conf);

        LOG.info("Creating DynamoDB client in region {}, endpoint {}",
                region, (StringUtils.isEmpty(endpoint) ? "DEFAULT" : endpoint));

        AmazonDynamoDBClientBuilder builder =
                AmazonDynamoDBClientBuilder.standard()
                .withCredentials(credentials)
                .withClientConfiguration(awsConf)
                .withRegion(region);

        if (!StringUtils.isEmpty(endpoint)) {
            builder = builder.withEndpointConfiguration(
                    new AwsClientBuilder.EndpointConfiguration(endpoint, region));
        }

        return builder.build();
    }

    static String getRegion(Configuration conf, String defaultRegion) {
        String region = conf.getTrimmed(S3GUARD_DDB_REGION_KEY);
        if (StringUtils.isEmpty(region)) {
            region = defaultRegion;
        }
        return region;
    }

    static String getEndpoint(Configuration conf) {
        return conf.getTrimmed(S3GUARD_DDB_ENDPOINT_KEY);
    }

}
