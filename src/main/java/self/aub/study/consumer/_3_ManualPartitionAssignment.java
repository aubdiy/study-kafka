package self.aub.study.consumer;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;

/**
 * Created by liujinxin on 16/7/28.
 */
public class _3_ManualPartitionAssignment extends _0_BaseConsumer {
    public static void main(String[] args) {
        String group = "manul-partition";
        KafkaConsumer<String, String> consumer = getConsumer(group, true);

        String topic = "test-topic";
        TopicPartition partition0 = new TopicPartition(topic, 0);

        consumer.assign(Arrays.asList(partition0));
        //TopicPartition partition1 = new TopicPartition(topic, 1);
        //consumer.assign(Arrays.asList(partition0, partition1));
        while (true) {
            printMessage(consumer);
        }
    }
}
