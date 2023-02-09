package ru.yandex.cloud.dataproc.sample.consumer;

import java.text.SimpleDateFormat;
import java.util.UUID;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/*

CLUSTER=normal-3
KAFKA=rc1c-amg5f4q074e2hshf.mdb.yandexcloud.net:9091

yc dataproc job create-spark --cluster-name ${CLUSTER} \
  --main-class ru.yandex.cloud.dataproc.sample.consumer.SampleConsumer \
  --main-jar-file-uri s3a://dproc-code/shared-libs/sample-consumer-1.0-SNAPSHOT.jar \
  --properties spark.dataproc.demo.kafka.file-prefix=s3a://dproc-wh/kafka1/sample- \
  --properties spark.dataproc.demo.kafka.bootstrap=${KAFKA} \
  --properties spark.dataproc.demo.kafka.user=user1 \
  --properties spark.dataproc.demo.kafka.password=jah5oeRu1B \
  --properties spark.dataproc.demo.kafka.topic=topic1 \
  --properties spark.dataproc.demo.kafka.mode=BATCH

yc dataproc job list --cluster-name ${CLUSTER}

yc dataproc job cancel --cluster-name ${CLUSTER} --id c9qo979jijmmba6nr5v8

*/

/**
 * Sample Kafka consumer writing the current set of records from topic to the Parquet file.
 * @author zinal
 */
public class SampleConsumer {

    public final static String PROP_FILE_PREFIX = "spark.dataproc.demo.kafka.file-prefix";
    public final static String PROP_KAFKA_BOOTSTRAP = "spark.dataproc.demo.kafka.bootstrap";
    public final static String PROP_KAFKA_USER = "spark.dataproc.demo.kafka.user";
    public final static String PROP_KAFKA_PASSWORD = "spark.dataproc.demo.kafka.password";
    public final static String PROP_KAFKA_TOPIC = "spark.dataproc.demo.kafka.topic";
    public final static String PROP_MODE = "spark.dataproc.demo.kafka.mode";

    public static enum ExecMode {
        BATCH,
        STREAM
    }

    private final SparkSession spark;
    private final ExecMode mode;
    private final StructType jsonType;

    public SampleConsumer(SparkSession spark) {
        this.spark = spark;
        this.mode = ExecMode.valueOf(spark.conf().get(PROP_MODE, ExecMode.BATCH.name()));
        this.jsonType = new StructType(new StructField[]{
            new StructField("a", DataTypes.StringType, false, Metadata.empty()),
            new StructField("b", DataTypes.LongType, false, Metadata.empty()),
            new StructField("c", DataTypes.IntegerType, false, Metadata.empty()),
            new StructField("d", DataTypes.StringType, false, Metadata.empty()),
        });
    }

    public static void main(String[] args) throws Exception {
        new SampleConsumer(SparkSession.builder()
                .appName("dataproc-sample-kafka-consumer").getOrCreate()).run();
    }

    public void run() throws Exception {
        switch (mode) {
            case BATCH:
                runBatch();
                break;
            case STREAM:
                runStream();
                break;
        }
    }

    public void runBatch() throws Exception {
        final Dataset<Row> ds1 = spark.read().format("kafka")
                .options(makeKafkaOptions()).load();
        final Dataset<Row> ds2 = ds1.select(
                functions.col("key").cast(DataTypes.StringType),
                functions.from_json(functions.col("value").cast(DataTypes.StringType), jsonType)
                        .alias("value")
        );
        final Dataset<Row> ds3 = ds2.select(
                functions.col("key"),
                functions.col("value.*")
        );
        // ds3.printSchema();
        ds3.write().format("parquet").save(makeOutputDirName());
    }

    public void runStream() throws Exception {
        final Dataset<Row> ds1 = spark.readStream().format("kafka")
                .options(makeKafkaOptions()).load();
        final Dataset<Row> ds2 = ds1.select(
                functions.col("key").cast(DataTypes.StringType),
                functions.from_json(functions.col("value").cast(DataTypes.StringType), jsonType)
                        .alias("value")
        );
        final Dataset<Row> ds3 = ds2.select(
                functions.col("key"),
                functions.col("value.*")
        );
        ds3.writeStream().format("parquet")
                .option("checkpointLocation", makeCheckpointLocation())
                .start(makeOutputDirName()).awaitTermination();
    }

    private java.util.Map<String, String> makeKafkaOptions() {
        final java.util.Map<String, String> map = new java.util.HashMap<>();
        map.put("kafka.sasl.mechanism", "SCRAM-SHA-512");
        map.put("kafka.security.protocol", "SASL_SSL");
        map.put("kafka.bootstrap.servers", spark.conf().get(PROP_KAFKA_BOOTSTRAP));
        map.put("kafka.sasl.jaas.config", makeJaasConfigString());
        map.put("subscribe", spark.conf().get(PROP_KAFKA_TOPIC));
        return map;
    }

    private String makeJaasConfigString() {
        return String.format("org.apache.kafka.common.security.scram.ScramLoginModule required"
                + " username=\"%s\" password=\"%s\";",
                spark.conf().get(PROP_KAFKA_USER), spark.conf().get(PROP_KAFKA_PASSWORD));
    }

    private String makeOutputDirName() {
        String prefix = spark.conf().get(PROP_FILE_PREFIX);
        if (prefix==null || prefix.length()==0)
            prefix = "/tmp/dataproc-kafka-data_";
        String date = new SimpleDateFormat("yyyy-MM-dd/HH-mm-ss").format(new java.util.Date());
        return prefix + date + "_" + UUID.randomUUID().toString();
    }

    private String makeCheckpointLocation() {
        String prefix = spark.conf().get(PROP_FILE_PREFIX);
        if (prefix==null || prefix.length()==0)
            prefix = "/tmp/dataproc-kafka-ckp_";
        return prefix + "_ckp";
    }

}
