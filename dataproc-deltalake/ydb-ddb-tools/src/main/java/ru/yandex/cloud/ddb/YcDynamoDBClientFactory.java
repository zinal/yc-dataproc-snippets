package ru.yandex.cloud.ddb;

import java.io.IOException;
import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.s3a.s3guard.DynamoDBClientFactory;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import static org.apache.hadoop.fs.s3a.Constants.S3GUARD_DDB_REGION_KEY;

/**
 * This class works generally like DynamoDBClientFactory.DefaultDynamoDBClientFactory,
 * but supports the additional configuraiton properties:
 *   * "fs.s3a.s3guard.ddb.endpoint" - to specify the DynamoDB endpoint to be used;
 *   * "fs.s3a.s3guard.ddb.lockbox" - Yandex Cloud Lockbox entry holding the AWS key id and secret;
 *   * "fs.s3a.s3guard.ddb.keyfile" - Java XML properties file holding the AWS key id and secret.
 * In addition, it does not honor the AWS ClientConfiguration.
 * The modified class allows YDB Serverless in Yandex Cloud to be used to serve DynamoDB requests of
 * S3Guard.
 *
 * The jar containing the code has to be added to the Hadoop jars, and enabled by setting
 * fs.s3a.s3guard.ddb.client.factory.impl = ru.yandex.cloud.s3guard.YcDynamoDBClientFactory
 *
 * @author zinal
 */
public class YcDynamoDBClientFactory extends Configured implements DynamoDBClientFactory {

    private static final org.slf4j.Logger LOG
            = org.slf4j.LoggerFactory.getLogger(YcDynamoDBClientFactory.class);

    public static final String S3GUARD_DDB_ENDPOINT_KEY = "fs.s3a.s3guard.ddb.endpoint";
    public static final String S3GUARD_DDB_LOCKBOX_KEY = "fs.s3a.s3guard.ddb.lockbox";
    public static final String S3GUARD_DDB_KEYFILE_KEY = "fs.s3a.s3guard.ddb.keyfile";

    @Override
    public AmazonDynamoDB createDynamoDBClient(String defaultRegion,
            String bucket, AWSCredentialsProvider credentials) throws IOException {
        Preconditions.checkNotNull(getConf(),
                "Should have been configured before usage");

        final Configuration conf = getConf();

        AmazonDynamoDBClientBuilder builder
                = AmazonDynamoDBClientBuilder.standard();

        final String keyfile = getKeyFile(conf);
        final String lockbox = getLockboxSecret(conf);
        final String endpoint = getEndpoint(conf);
        final String region = getRegion(conf, defaultRegion);

        if (StringUtils.isEmpty(endpoint)) {
            LOG.debug("Creating DynamoDB client with region {}", region);
            builder = builder.withRegion(region);
        } else {
            LOG.debug("Creating DynamoDB client with explicit endpoint {}", endpoint);
            builder = builder.disableEndpointDiscovery();
            builder = builder.withEndpointConfiguration(
                    new AwsClientBuilder.EndpointConfiguration(endpoint, region));
        }

        if (!StringUtils.isEmpty(keyfile)) {
            LOG.debug("Reading the AWS credentials from key file {}", keyfile);
            builder = builder.withCredentials(new YcFileAwsCredentials(keyfile));
        } else if (!StringUtils.isEmpty(lockbox)) {
            LOG.debug("Reading the AWS credentials from Lockbox entry {}", lockbox);
            builder = builder.withCredentials(new YcLockboxAwsCredentials(lockbox));
        } else {
            LOG.debug("Using the default input AWS credentials");
            builder = builder.withCredentials(credentials);
        }

        return builder.build();
    }

    static String getKeyFile(Configuration conf) {
        return conf.getTrimmed(S3GUARD_DDB_KEYFILE_KEY);
    }

    static String getLockboxSecret(Configuration conf) {
        return conf.getTrimmed(S3GUARD_DDB_LOCKBOX_KEY);
    }

    static String getEndpoint(Configuration conf) {
        return conf.getTrimmed(S3GUARD_DDB_ENDPOINT_KEY);
    }

    static String getRegion(Configuration conf, String defaultRegion)
            throws IOException {
        String region = conf.getTrimmed(S3GUARD_DDB_REGION_KEY);
        if (StringUtils.isEmpty(region)) {
            region = defaultRegion;
        }
        return region;
    }
}
