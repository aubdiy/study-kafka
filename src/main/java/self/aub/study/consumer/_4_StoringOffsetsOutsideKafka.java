package self.aub.study.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;

import static self.aub.study.consumer._0_BaseConsumer.getConsumer;

/**
 * Created by liujinxin on 16/7/28.
 */
public class _4_StoringOffsetsOutsideKafka {
    public static void main(String[] args) {
        String group = "automatic";
        KafkaConsumer<String, String> consumer = getConsumer(group, false);

        String topic = "test-topic";
        TopicPartition partition0 = new TopicPartition(topic, 0);

        consumer.assign(Arrays.asList(partition0));

        long currentOffset = 0;

        while (true) {
            System.out.println("=========");

            consumer.seek(partition0, currentOffset);

            ConsumerRecords<String, String> records = consumer.poll(100);
            long tmpOffset = currentOffset - 1;
            for (ConsumerRecord<String, String> record : records) {
                int partition = record.partition();
                long offset = record.offset();
                tmpOffset = offset;
                String key = record.key();
                String value = record.value();
                long timestamp = record.timestamp();
                System.out.printf("timestamp = %d, partition = %d, offset = %d, key = %s, value = %s\n", timestamp, partition, offset, key, value);
            }
            currentOffset = tmpOffset + 1;
        }

    }
}
