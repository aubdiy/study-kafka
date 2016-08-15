package self.aub.study.producer;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;
import java.util.Random;

public class OldProducer {
    private static String brokers = "127.0.0.1:10116";

    private static Producer<String, String> genProducer(String brokers) {
        Properties props = new Properties();
        props.put("metadata.broker.list", brokers);
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("request.required.acks", "1");
//                props.put("partitioner.class", "self.aub.study.kafka.RandomPartitioner");
        ProducerConfig config = new ProducerConfig(props);
        return new Producer<>(config);
    }


    private static void appendCeaseless(final String topicName) {
        new Thread(new Runnable() {
            @Override
            public void run() {
                Properties props = new Properties();
                props.put("metadata.broker.list", brokers);
                props.put("serializer.class", "kafka.serializer.StringEncoder");
                props.put("request.required.acks", "1");
//                props.put("partitioner.class", "self.aub.study.kafka.RandomPartitioner");
                ProducerConfig config = new ProducerConfig(props);
                Producer<String, String> producer = new Producer<>(config);

                Random random = new Random();
                while (true) {

                    String data = "data==" + topicName + ">>" + random.nextInt(1024);
                    System.out.println(Thread.currentThread().getName() + "   : " + data);
                    KeyedMessage<String, String> km = new KeyedMessage<>(topicName, data);
                    producer.send(km);
                    try {
                        Thread.sleep(2000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }).start();
    }

    public static void appendOnce(String topicName, String data) {
        Properties props = new Properties();
        props.put("metadata.broker.list", brokers);
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("request.required.acks", "1");
        //props.put("partitioner.class", "self.aub.study.kafka.RandomPartitioner");

        ProducerConfig config = new ProducerConfig(props);
        Producer<String, String> producer = new Producer<>(config);

        System.out.println(Thread.currentThread().getName() + "   : " + data);
        KeyedMessage<String, String> km = new KeyedMessage<>(topicName, null, data);
        producer.send(km);
        producer.close();
    }


    public static void main(String[] args) throws Exception {
        appendCeaseless("test1");

        appendOnce("test_topic", "123123123");



    }
}
