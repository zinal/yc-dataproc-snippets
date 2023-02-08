package ru.yandex.cloud.dataproc.sample.consumer;

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

KAFKA=rc1a-0n96ifqmlou7f9gi.mdb.yandexcloud.net:9091,rc1b-856cmu9b98qojh0i.mdb.yandexcloud.net:9091,rc1c-aglv7qressjalsau.mdb.yandexcloud.net:9091

yc dataproc job create-spark --cluster-name normal-3 \
  --main-class ru.yandex.cloud.dataproc.sample.consumer.SampleConsumer \
  --main-jar-file-uri s3a://dproc-code/shared-libs/sample-consumer-1.0-SNAPSHOT.jar \
  --properties spark.dataproc.demo.kafka.file-prefix=s3a://dproc-wh/kafka1/sample- \
  --properties spark.dataproc.demo.kafka.bootstrap=${KAFKA} \
  --properties spark.dataproc.demo.kafka.user=user1 \
  --properties spark.dataproc.demo.kafka.password=jah5oeRu1B \
  --properties spark.dataproc.demo.kafka.topic=topic1

yc dataproc job list --cluster-name normal-3

yc dataproc job cancel --cluster-name normal-3 --id c9qo979jijmmba6nr5v8

*/

/**
 * Sample Kafka consumer writing the current set of records from topic to the Parquet file.
 * @author zinal
 */
public class SampleConsumer implements Runnable {

    public final static String PROP_FILE_PREFIX = "spark.dataproc.demo.kafka.file-prefix";
    public final static String PROP_KAFKA_BOOTSTRAP = "spark.dataproc.demo.kafka.bootstrap";
    public final static String PROP_KAFKA_USER = "spark.dataproc.demo.kafka.user";
    public final static String PROP_KAFKA_PASSWORD = "spark.dataproc.demo.kafka.password";
    public final static String PROP_KAFKA_TOPIC = "spark.dataproc.demo.kafka.topic";

    private final SparkSession spark;

    public SampleConsumer(SparkSession spark) {
        this.spark = spark;
    }

    public static void main(String[] args) {
        new SampleConsumer(SparkSession.builder()
                .appName("dataproc-sample-kafka-consumer").getOrCreate()).run();
    }

    @Override
    public void run() {
        final StructType jsonType = new StructType(new StructField[]{
            new StructField("a", DataTypes.StringType, false, Metadata.empty()),
            new StructField("b", DataTypes.LongType, false, Metadata.empty()),
            new StructField("c", DataTypes.IntegerType, false, Metadata.empty()),
            new StructField("d", DataTypes.StringType, false, Metadata.empty()),
        });
        final Dataset<Row> ds1 = spark.read().format("kafka").options(makeKafkaOptions()).load();
        final Dataset<Row> ds2 = ds1.select(
                functions.col("key").cast(DataTypes.StringType),
                functions.from_json(functions.col("value").cast(DataTypes.StringType), jsonType)
                        .alias("value")
        );
        ds2.write().format("parquet").save(makeOutputFileName());
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

    private String makeOutputFileName() {
        String prefix = spark.conf().get(PROP_FILE_PREFIX);
        if (prefix==null || prefix.length()==0)
            prefix = "/tmp/dataproc-kafka-";
        return prefix + UUID.randomUUID().toString() + ".parquet";
    }

}
