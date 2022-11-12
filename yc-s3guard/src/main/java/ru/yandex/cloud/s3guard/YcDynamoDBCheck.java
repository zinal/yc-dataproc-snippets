package ru.yandex.cloud.s3guard;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import java.io.IOException;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import static org.apache.hadoop.fs.s3a.Constants.S3GUARD_DDB_REGION_KEY;
import org.apache.hadoop.fs.s3a.S3AUtils;
import static ru.yandex.cloud.s3guard.YcDynamoDBClientFactory.S3GUARD_DDB_ENDPOINT_KEY;
import static ru.yandex.cloud.s3guard.YcDynamoDBClientFactory.S3GUARD_DDB_KEYFILE_KEY;
import static ru.yandex.cloud.s3guard.YcDynamoDBClientFactory.S3GUARD_DDB_LOCKBOX_KEY;

/**
 *
 * @author zinal
 */
public class YcDynamoDBCheck {

    private static final org.slf4j.Logger LOG
            = org.slf4j.LoggerFactory.getLogger(YcDynamoDBCheck.class);

    public YcDynamoDBCheck() {
    }

    public void run() throws Exception {
        String endpoint = "https://docapi.serverless.yandexcloud.net/ru-central1/b1gfvslmokutuvt2g019/etnuogblap3e7dok6tf5";
        String region = "ru-central1";
        String keyfile = "keyfile.tmp";

        final ClientConfiguration awsConf = new ClientConfiguration();

        AmazonDynamoDBClientBuilder builder
                = AmazonDynamoDBClientBuilder.standard();
        builder = builder.disableEndpointDiscovery();
        builder = builder.withEndpointConfiguration(
                new AwsClientBuilder.EndpointConfiguration(endpoint, region));
        builder = builder.withCredentials(new YcFileAwsCredentialsProvider(keyfile));

        AmazonDynamoDB addb = builder.withClientConfiguration(awsConf).build();
        DynamoDB ddb = new DynamoDB(addb);
        Table table = ddb.getTable("test123");
        table.describe();
        System.out.println("Okay...");
    }

    public static void main(String[] args) {
        try {
            new YcDynamoDBCheck().run();
        } catch(Exception ex) {
            ex.printStackTrace(System.err);
            System.exit(1);
        }
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
