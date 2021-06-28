package kafka;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;


public class KafkaUtilities {
    static String topic = "test";
    static String key  = "alice";
    static ClassLoader loader = Thread.currentThread().getContextClassLoader();


    public static class MyThread extends Thread {

        public void run(){
            System.out.println("MyThread running");
            try {
                consumer();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    public static void main(String[] args) throws IOException, InterruptedException {
        InputStream kafka_file = loader.getResourceAsStream("kafka.properties");
        Properties props = new Properties();
        props.load(kafka_file);
        createTopic(topic, props);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "flink-producer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());





        Producer<String, Integer> producer = new KafkaProducer<String, Integer>(props);
        producer.send(new ProducerRecord<String, Integer>(topic, key, 11), new Callback() {
            @Override
            public void onCompletion(RecordMetadata m, Exception e) {
                if (e != null) {
                    e.printStackTrace();
                } else {
                    System.out.printf("Produced record to topic %s partition [%d] @ offset %d%n", m.topic(),
                            m.partition(), m.offset());
                }
            }
        });
        producer.send(new ProducerRecord<String, Integer>(topic, key+"1", 12), new Callback() {
            @Override
            public void onCompletion(RecordMetadata m, Exception e) {
                if (e != null) {
                    e.printStackTrace();
                } else {
                    System.out.printf("Produced record to topic %s partition [%d] @ offset %d%n", m.topic(),
                            m.partition(), m.offset());
                }
            }
        });
        Thread.sleep(100);
        //MyThread myThread = new MyThread();
        //myThread.run();
    }



    static void consumer() throws IOException {
        InputStream kafka_file = loader.getResourceAsStream("kafka.properties");
        Properties props = new Properties();
        props.load(kafka_file);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "flink");
        // start reading from beginning of partition if no offset was created
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // exactly once semantic
        //props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, true);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        Consumer<String, Integer> consumer = new KafkaConsumer<String, Integer>(props);
        consumer.subscribe(Arrays.asList(topic));
        Integer total_count = 0;

        try {
            while (true) {
                ConsumerRecords<String, Integer> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, Integer> record : records) {
                    String key = record.key();
                    Integer value = record.value();
                    total_count += value;
                    System.out.printf("Consumed record with key %s and value %s, and updated total count to %d%n", key, value, total_count);
                }
            }
        } finally {
            consumer.close();
        }
    }

    // Create topic in Confluent Cloud
    public static void createTopic(final String topic,
                                   final Properties cloudConfig) {
        final NewTopic newTopic = new NewTopic(topic, Optional.empty(), Optional.empty());
        try (final AdminClient adminClient = AdminClient.create(cloudConfig)) {
            adminClient.createTopics(Collections.singletonList(newTopic)).all().get();
        } catch (final InterruptedException | ExecutionException e) {
            // Ignore if TopicExistsException, which may be valid if topic exists
            if (!(e.getCause() instanceof TopicExistsException)) {
                throw new RuntimeException(e);
            }
        }
    }

    public static Properties loadConfig(final String configFile) throws IOException {
        if (!Files.exists(Paths.get(configFile))) {
            throw new IOException(configFile + " not found.");
        }
        final Properties cfg = new Properties();
        try (InputStream inputStream = new FileInputStream(configFile)) {
            cfg.load(inputStream);
        }
        return cfg;
    }
}
