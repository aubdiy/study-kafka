package self.aub.study.consumer;

import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;

/**
 * Created by liujinxin on 16/7/27.
 */
public class _1_AutomaticOffsetCommitting extends _0_BaseConsumer {

    public static void main(String[] args) {
        String topic = "test";
        //String topic = "test-topic";
        String group = "automatic";
        KafkaConsumer<String, String> consumer = getConsumer(group, true);

        consumer.subscribe(Arrays.asList(topic));

        while (true) {
            printMessage(consumer);
        }

        //try {
        //    while(true) {
        //        ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
        //        for (TopicPartition partition : records.partitions()) {
        //            List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
        //            for (ConsumerRecord<String, String> record : partitionRecords) {
        //                System.out.println(record.offset() + ": " + record.value());
        //            }
        //            long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
        //            consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)));
        //        }
        //    }
        //} finally {
        //    consumer.close();
        //}
    }


}
