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
public class SampleProducer implements Runnable {

    private final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(SampleProducer.class);

    private final String topicName;
    private final double messageRate;
    private final Producer<String, String> producer;
    private final AtomicLong counter = new AtomicLong(System.currentTimeMillis());

    public SampleProducer(Properties props) {
        this.topicName = props.getProperty("demo.topic");
        double r = Double.parseDouble(props.getProperty("demo.msg.rate", "10.0"));
        if (r < 1.0)
            r = 1.0;
        this.messageRate = r;
        this.producer = new KafkaProducer<>(props);
    }

    /**
     * Generate a single Kafka message with the JSON payload.
     * @return The generated Kafka message
     */
    @SuppressWarnings("unchecked")
    public ProducerRecord<String, String> makeRecord() {
        JSONObject data = new JSONObject();
        String key = UUID.randomUUID().toString();
        final long v = counter.incrementAndGet();
        data.put("a", key);
        data.put("b", v % 10000L);
        data.put("c", v % 763L + 512);
        data.put("d", UUID.randomUUID().toString());
        return new ProducerRecord<>(topicName, key, data.toJSONString());
    }

    @Override
    public void run() {
        LOG.info("Now sending with rate {}", messageRate);

        final int maxPack;
        if (messageRate < 100.0)
            maxPack = 10;
        else
            maxPack = 100;

        int slotMessages = 0;
        long totalMessages = 0L;
        long tvStep = System.currentTimeMillis();
        while (true) {
            for (int packMessages=0; packMessages<maxPack; ++packMessages) {
                producer.send(makeRecord());
            }
            producer.flush();
            slotMessages += maxPack;
            totalMessages += maxPack;
            long tvCur;
            while (true) {
                tvCur = System.currentTimeMillis();
                final double curRate = (((double)slotMessages) * 1000.0) / ((double)(tvCur - tvStep));
                if (curRate <= messageRate*1.1)
                    break;
                try { Thread.sleep(10L); } catch(InterruptedException ix) {}
            }
            if ((tvCur - tvStep) >= 30000L) {
                LOG.info("...sent messages: {}, total: {}", slotMessages, totalMessages);
                slotMessages = 0;
                tvStep = tvCur;
            }
        }
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
            new SampleProducer(props).run();
        } catch(Exception ex) {
            ex.printStackTrace(System.err);
            System.exit(1);
        }
    }

}
