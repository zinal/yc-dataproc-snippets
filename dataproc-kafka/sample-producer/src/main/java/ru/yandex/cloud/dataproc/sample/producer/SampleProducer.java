package ru.yandex.cloud.dataproc.sample.producer;

import java.io.FileInputStream;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.clients.producer.*;
import org.json.simple.JSONObject;

/**
 * Simple Kafka JSON messages producer.
 * @author mzinal
 */
public class SampleProducer {

    private final static AtomicLong COUNTER = new AtomicLong(System.currentTimeMillis());

    public static ProducerRecord<String, String> makeRecord(String topicName) {
        JSONObject data = new JSONObject();
        String key = UUID.randomUUID().toString();
        long counter = COUNTER.incrementAndGet();
        data.put("a", key);
        data.put("b", counter % 10000L);
        data.put("c", counter % 763L + 512);
        data.put("d", UUID.randomUUID().toString());
        return new ProducerRecord<String, String>(topicName, key, data.toJSONString());
    }

    public static void main(String[] args) {
        if (args.length!=1) {
            System.out.println("USAGE: " + SampleProducer.class.getCanonicalName() + " jobfile.xml");
            System.exit(2);
        }
        try {
            final Properties props = new Properties();
            try (FileInputStream fis = new FileInputStream(args[0])) {
                props.loadFromXML(fis);
            }
            final String topicName = props.getProperty("demo.topic");
            double messageRate = Double.valueOf(props.getProperty("demo.msg.rate", "10.0"));
            if (messageRate < 1.0)
                messageRate = 1.0;

            System.out.println("Now sending with rate: " + String.valueOf(messageRate));

            final Producer<String, String> producer = new KafkaProducer<>(props);

            int packMessages = 0;
            int slotMessages = 0;
            long totalMessages = 0L;
            long tvStep = System.currentTimeMillis();
            while (true) {
                producer.send(makeRecord(topicName));
                packMessages += 1;
                slotMessages += 1;
                totalMessages += 1;
                if (packMessages >= 10) {
                    producer.flush();
                    packMessages = 0;
                }
                long tvCur;
                while (true) {
                    tvCur = System.currentTimeMillis();
                    final double curRate = ((double)slotMessages) /
                            (((double)(tvCur - tvStep)) / 1000.0);
                    if (curRate < messageRate)
                        break;
                    try { Thread.sleep(70L); } catch(InterruptedException ix) {}
                }
                if ((tvCur - tvStep) >= 30000L) {
                    packMessages = 0;
                    tvStep = tvCur;
                    System.out.println("...sent messages: " + String.valueOf(totalMessages));
                }
            }
            

        } catch(Exception ex) {
            ex.printStackTrace(System.err);
            System.exit(1);
        }
    }

}
