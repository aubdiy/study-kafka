package self.aub.study.confluent;

import io.confluent.kafka.serializers.KafkaAvroDecoder;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import kafka.utils.VerifiableProperties;
import org.apache.avro.generic.IndexedRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by liujinxin on 16/8/2.
 */
public class ConfluentConsumer {


    public static void main(String[] args) {
        String topic = "test";
        String group = "confluent_avro1";
        //oldConsumer(topic, group);
        newConsumer(topic, group);
    }

    private static void oldConsumer(String topic, String group) {

        String zookeeper = "127.0.0.1:2181/kafka_2.11-0.10";

        Properties props = new Properties();
        props.put("zookeeper.connect", zookeeper);
        props.put("group.id", group);
        props.put("schema.registry.url", "http://localhost:8081");

        Map<String, Integer> topicCountMap = new HashMap<>();
        topicCountMap.put(topic, new Integer(1));

        VerifiableProperties vProps = new VerifiableProperties(props);
        KafkaAvroDecoder keyDecoder = new KafkaAvroDecoder(vProps);
        KafkaAvroDecoder valueDecoder = new KafkaAvroDecoder(vProps);

        ConsumerConfig consumerConfig = new ConsumerConfig(props);
        ConsumerConnector consumer = Consumer.createJavaConsumerConnector(consumerConfig);
        Map<String, List<KafkaStream<Object, Object>>> consumerMap = consumer.createMessageStreams(topicCountMap, keyDecoder, valueDecoder);
        KafkaStream stream = consumerMap.get(topic).get(0);
        ConsumerIterator it = stream.iterator();
        while (it.hasNext()) {
            MessageAndMetadata messageAndMetadata = it.next();
            int partition = messageAndMetadata.partition();
            long offset = messageAndMetadata.offset();
            String key = (String) messageAndMetadata.key();
            IndexedRecord value = (IndexedRecord) messageAndMetadata.message();
            System.out.printf("partition = %d, offset = %d, key = %s, value = %s\n", partition, offset, key, value);
        }
    }


    private static void newConsumer(String topic,String group) {

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", group);
        props.put("enable.auto.commit", true);
        props.put("auto.commit.interval.ms", "1000");
        props.put("max.partition.fetch.bytes", "256");

        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer");
        props.put("value.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer");

        props.put("schema.registry.url", "http://localhost:8081");

        KafkaConsumer<Object, Object> consumer = new KafkaConsumer<>(props);

        consumer.subscribe(Arrays.asList(topic));
        while (true) {
            ConsumerRecords<Object, Object> records = consumer.poll(100);
            for (ConsumerRecord<Object, Object> record : records) {
                int partition = record.partition();
                long offset = record.offset();
                Object key = record.key();
                IndexedRecord value = (IndexedRecord)record.value();
                long timestamp = record.timestamp();
                System.out.printf("timestamp = %d, partition = %d, offset = %d, key = %s, value = %s\n", timestamp, partition, offset, key, value);
            }
        }
    }
}
