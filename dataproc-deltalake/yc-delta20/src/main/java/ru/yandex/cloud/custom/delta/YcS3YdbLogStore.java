package ru.yandex.cloud.custom.delta;

import ru.yandex.cloud.custom.ddb.YcLockboxAwsCredentials;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import io.delta.storage.BaseExternalLogStore;
import io.delta.storage.ExternalCommitEntry;
import io.delta.storage.S3DynamoDBLogStore;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.*;

import ru.yandex.cloud.custom.ddb.YcLockboxAwsCredentials;

/**
 * A customized implementation of {@link S3DynamoDBLogStore} that works with
 * YDB as a DynamoDB substitute in Yandex Cloud.
 *
 * Initially copied from Delta Lake 2.0.2, original code reference:
 * https://github.com/delta-io/delta/blob/v2.0.2/storage-s3-dynamodb/src/main/java/io/delta/storage/S3DynamoDBLogStore.java
 *
 * Adopted for YDB by Maksim Zinal.
 */
public class YcS3YdbLogStore extends BaseExternalLogStore {

    private static final Logger LOG = LoggerFactory.getLogger(S3DynamoDBLogStore.class);

    // 1.0-2023.02.26
    // 1.1-SNAPSHOT
    public static final String VERSION = "YcS3YdbLogStore 1.0-2023.07.14";

    /**
     * Configuration keys for the DynamoDB client.
     *
     * Keys are either of the form $SPARK_CONF_PREFIX.$CONF or $BASE_CONF_PREFIX.$CONF,
     * e.g. spark.io.delta.storage.S3DynamoDBLogStore.ddb.tableName
     * or io.delta.storage.S3DynamoDBLogStore.ddb.tableName
     */
    public static final String SPARK_CONF_PREFIX = "spark.io.delta.storage.S3DynamoDBLogStore";
    public static final String BASE_CONF_PREFIX = "io.delta.storage.S3DynamoDBLogStore";
    public static final String DDB_ENDPOINT = "ddb.endpoint";
    public static final String DDB_LOCKBOX = "ddb.lockbox";
    public static final String DDB_CLIENT_TABLE = "ddb.tableName";
    public static final String DDB_CLIENT_REGION = "ddb.region";

    // WARNING: setting this value too low can cause data loss. Defaults to a duration of 1 day.
    public static final String TTL_SECONDS = "ddb.ttl";

    /**
     * DynamoDB table attribute keys
     */
    private static final String ATTR_TABLE_PATH = "tablePath";
    private static final String ATTR_FILE_NAME = "fileName";
    private static final String ATTR_TEMP_PATH = "tempPath";
    private static final String ATTR_COMPLETE = "complete";
    private static final String ATTR_EXPIRE_TIME = "expireTime";

    /**
     * Member fields
     */
    private final AmazonDynamoDB client;
    private final String tableName;
    private final String regionName;
    private final String endpoint;
    private final String lockboxEntry;
    private final long expirationDelaySeconds;

    public YcS3YdbLogStore(Configuration hadoopConf) throws IOException {
        super(hadoopConf);

        tableName = getParam(hadoopConf, DDB_CLIENT_TABLE, "delta_log");
        regionName = getParam(hadoopConf, DDB_CLIENT_REGION, "ru-central1");
        endpoint = getParam(hadoopConf, DDB_ENDPOINT, "MISSING");
        lockboxEntry = getParam(hadoopConf, DDB_LOCKBOX, "MISSING");

        final String ttl = getParam(hadoopConf, TTL_SECONDS, null);
        expirationDelaySeconds = ttl == null ?
            BaseExternalLogStore.DEFAULT_EXTERNAL_ENTRY_EXPIRATION_DELAY_SECONDS :
            Long.parseLong(ttl);
        if (expirationDelaySeconds < 0) {
            throw new IllegalArgumentException(
                String.format(
                    "Can't use negative `%s` value of %s", TTL_SECONDS, expirationDelaySeconds));
        }

        LOG.info("using tableName {}", tableName);
        LOG.info("using regionName {}", regionName);
        LOG.info("using endpoint {}", endpoint);
        LOG.info("using lockboxEntry {}", lockboxEntry);
        LOG.info("using ttl (seconds) {}", expirationDelaySeconds);

        client = getClient();
        tryEnsureTableExists(hadoopConf);
    }

    @Override
    protected long getExpirationDelaySeconds() {
        return expirationDelaySeconds;
    }

    @Override
    protected void putExternalEntry(
            ExternalCommitEntry entry,
            boolean overwrite) throws IOException {
        try {
            if (LOG.isDebugEnabled()) {
                LOG.debug(String.format("putItem %s, overwrite: %s", entry, overwrite));
            }
            client.putItem(createPutItemRequest(entry, overwrite));
        } catch (ConditionalCheckFailedException e) {
            LOG.debug(e.toString());
            throw new java.nio.file.FileAlreadyExistsException(
                entry.absoluteFilePath().toString()
            );
        }
    }

    @Override
    protected Optional<ExternalCommitEntry> getExternalEntry(
            String tablePath,
            String fileName) {
        final Map<String, AttributeValue> attributes = new ConcurrentHashMap<>();
        attributes.put(ATTR_TABLE_PATH, new AttributeValue(tablePath));
        attributes.put(ATTR_FILE_NAME, new AttributeValue(fileName));

        Map<String, AttributeValue> item = client.getItem(
            new GetItemRequest(tableName, attributes).withConsistentRead(true)
        ).getItem();

        return item != null ? Optional.of(dbResultToCommitEntry(item)) : Optional.empty();
    }

    @Override
    protected Optional<ExternalCommitEntry> getLatestExternalEntry(Path tablePath) {
        final Map<String, Condition> conditions = new ConcurrentHashMap<>();
        conditions.put(
            ATTR_TABLE_PATH,
            new Condition()
                .withComparisonOperator(ComparisonOperator.EQ)
                .withAttributeValueList(new AttributeValue(tablePath.toString()))
        );

        final List<Map<String,AttributeValue>> items = client.query(
            new QueryRequest(tableName)
                .withConsistentRead(true)
                .withScanIndexForward(false)
                .withLimit(1)
                .withKeyConditions(conditions)
        ).getItems();

        if (items.isEmpty()) {
            return Optional.empty();
        } else {
            return Optional.of(dbResultToCommitEntry(items.get(0)));
        }
    }

    /**
     * Map a DBB query result item to an {@link ExternalCommitEntry}.
     */
    private ExternalCommitEntry dbResultToCommitEntry(Map<String, AttributeValue> item) {
        final AttributeValue expireTimeAttr = item.get(ATTR_EXPIRE_TIME);
        return new ExternalCommitEntry(
            new Path(item.get(ATTR_TABLE_PATH).getS()),
            item.get(ATTR_FILE_NAME).getS(),
            item.get(ATTR_TEMP_PATH).getS(),
            item.get(ATTR_COMPLETE).getS().equals("true"),
            expireTimeAttr != null ? Long.parseLong(expireTimeAttr.getN()) : null
        );
    }

    private PutItemRequest createPutItemRequest(ExternalCommitEntry entry, boolean overwrite) {
        final Map<String, AttributeValue> attributes = new ConcurrentHashMap<>();
        attributes.put(ATTR_TABLE_PATH, new AttributeValue(entry.tablePath.toString()));
        attributes.put(ATTR_FILE_NAME, new AttributeValue(entry.fileName));
        attributes.put(ATTR_TEMP_PATH, new AttributeValue(entry.tempPath));
        attributes.put(
            ATTR_COMPLETE,
            new AttributeValue().withS(Boolean.toString(entry.complete))
        );

        if (entry.expireTime != null) {
            attributes.put(
                ATTR_EXPIRE_TIME,
                new AttributeValue().withN(entry.expireTime.toString())
            );
        }

        final PutItemRequest pr = new PutItemRequest(tableName, attributes);

        if (!overwrite) {
            Map<String, ExpectedAttributeValue> expected = new ConcurrentHashMap<>();
            expected.put(ATTR_FILE_NAME, new ExpectedAttributeValue(false));
            pr.withExpected(expected);
        }

        return pr;
    }

    private void tryEnsureTableExists(Configuration hadoopConf) throws IOException {
        int retries = 0;
        boolean created = false;
        while(retries < 20) {
            String status = "CREATING";
            try {
                // https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/dynamodbv2/model/TableDescription.html#getTableStatus--
                DescribeTableResult result = client.describeTable(tableName);
                TableDescription descr = result.getTable();
                status = descr.getTableStatus();
            } catch (ResourceNotFoundException e) {
                final long rcu = 5; // no need to customize for YDB
                final long wcu = 5;

                LOG.info(
                    "DynamoDB table `{}` in region `{}` does not exist. " +
                    "Creating it now with provisioned throughput of {} RCUs and {} WCUs.",
                    tableName, regionName, rcu, wcu);
                try {
                    client.createTable(
                        // attributeDefinitions
                        java.util.Arrays.asList(
                            new AttributeDefinition(ATTR_TABLE_PATH, ScalarAttributeType.S),
                            new AttributeDefinition(ATTR_FILE_NAME, ScalarAttributeType.S)
                        ),
                        tableName,
                        // keySchema
                        Arrays.asList(
                            new KeySchemaElement(ATTR_TABLE_PATH, KeyType.HASH),
                            new KeySchemaElement(ATTR_FILE_NAME, KeyType.RANGE)
                        ),
                        new ProvisionedThroughput(rcu, wcu)
                    );
                    created = true;
                } catch (ResourceInUseException e3) {
                    // race condition - table just created by concurrent process
                }
            }
            if (status.equals("ACTIVE")) {
                if (created) {
                    LOG.info("Successfully created DynamoDB table `{}`", tableName);
                } else {
                    LOG.info("Table `{}` already exists", tableName);
                }
                break;
            } else if (status.equals("CREATING")) {
                retries += 1;
                LOG.info("Waiting for `{}` table creation", tableName);
                try {
                    Thread.sleep(1000);
                } catch(InterruptedException e) {
                    throw new InterruptedIOException(e.getMessage());
                }
            } else {
                LOG.error("table `{}` status: {}", tableName, status);
                break;  // TODO - raise exception?
            }
        }
    }

    private AmazonDynamoDB getClient() {
        // Yandex Cloud and YDB specific configuration.
        return AmazonDynamoDBClientBuilder.standard()
                .disableEndpointDiscovery()
                .withEndpointConfiguration(
                    new AwsClientBuilder.EndpointConfiguration(endpoint, regionName))
                .withCredentials(new YcLockboxAwsCredentials(lockboxEntry))
                .build();
    }

    /**
     * Get the hadoopConf param $name that is prefixed either with $SPARK_CONF_PREFIX or
     * $BASE_CONF_PREFIX.
     *
     * If two parameters exist, one for each prefix, then an IllegalArgumentException is thrown.
     *
     * If no parameters exist, then the $defaultValue is returned.
     */
    protected static String getParam(Configuration hadoopConf, String name, String defaultValue) {
        final String sparkPrefixKey = String.format("%s.%s", SPARK_CONF_PREFIX, name);
        final String basePrefixKey = String.format("%s.%s", BASE_CONF_PREFIX, name);

        final String sparkPrefixVal = hadoopConf.get(sparkPrefixKey);
        final String basePrefixVal = hadoopConf.get(basePrefixKey);

        if (sparkPrefixVal != null &&
            basePrefixVal != null &&
            !sparkPrefixVal.equals(basePrefixVal)) {
            throw new IllegalArgumentException(
                String.format(
                    "Configuration properties `%s=%s` and `%s=%s` have different values. " +
                    "Please set only one.",
                    sparkPrefixKey, sparkPrefixVal, basePrefixKey, basePrefixVal
                )
            );
        }

        if (sparkPrefixVal != null) return sparkPrefixVal;
        if (basePrefixVal != null) return basePrefixVal;
        return defaultValue;
    }

}
