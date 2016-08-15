package self.aub.study.consumer;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class OldConsumer {

    //bin/kafka-topics.sh --create --zookeeper 127.0.0.1:2181/kafka_2.11-0.10 --replication-factor 1 --partitions 1 --topic test-topic
    //bin/kafka-console-producer.sh --broker-list 127.0.0.1:9092 --topic test-topic
    public static void main(String[] args) throws InterruptedException {
        String topic = "test";
        //String topic = "test-topic";
        String group = "test-group";
		String zookeeper="127.0.0.1:2181/kafka_2.11-0.10";

        Properties props = new Properties();
        props.put("zookeeper.connect", zookeeper);
        props.put("group.id", group);
        props.put("zookeeper.session.timeout.ms", "10000");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
        props.put("auto.offset.reset", "smallest");

        ConsumerConfig consumerConfig = new ConsumerConfig(props);
        ConsumerConnector consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();

        topicCountMap.put(topic, new Integer(1));
        StringDecoder keyDecoder = new StringDecoder(new VerifiableProperties());
        StringDecoder valueDecoder = new StringDecoder(new VerifiableProperties());

        Map<String, List<KafkaStream<String, String>>> consumerMap = consumerConnector.createMessageStreams(topicCountMap,keyDecoder,valueDecoder);
        List<KafkaStream<String, String>> streams = consumerMap.get(topic);


        //Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumerConnector
        //        .createMessageStreams(topicCountMap);
        //
        //List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);
        ExecutorService executor = Executors.newFixedThreadPool(1);


        //for (final KafkaStream<byte[], byte[]> stream : streams) {
        for (final KafkaStream<String, String> stream : streams) {
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    ConsumerIterator<String, String> iterator = stream.iterator();
                    System.out.println("===>");
                    while (iterator.hasNext()) {
                        MessageAndMetadata<String, String> next = iterator.next();
                        int partition = next.partition();
                        long offset = next.offset();
                        String key = next.key();
                        String value = next.message();
                        System.out.printf("partition = %d, offset = %d, key = %s, value = %s\n", partition, offset, key, value);
                    }

                }
            });
        }

        Thread.sleep(Integer.MAX_VALUE);
    }
}
