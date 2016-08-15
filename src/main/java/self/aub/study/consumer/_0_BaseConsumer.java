package self.aub.study.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Properties;

/**
 * Created by liujinxin on 16/7/28.
 */
public class _0_BaseConsumer {

    static KafkaConsumer<String, String> getConsumer(String group, boolean enableAutoCommit) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", group);
        props.put("enable.auto.commit", enableAutoCommit);
        props.put("auto.commit.interval.ms", "1000");
        props.put("max.partition.fetch.bytes", "256");

        props.put("offsets.storage", "zookeeper");
        props.put("dual.commit.enabled", false);


        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return new KafkaConsumer<>(props);
    }

    static void printMessage(KafkaConsumer<String, String> consumer) {
        ConsumerRecords<String, String> records = consumer.poll(100);
        for (ConsumerRecord<String, String> record : records) {
            int partition = record.partition();
            long offset = record.offset();
            String key = record.key();
            String value = record.value();
            long timestamp = record.timestamp();
            System.out.printf("timestamp = %d, partition = %d, offset = %d, key = %s, value = %s\n", timestamp, partition, offset, key, value);
        }
    }
}
