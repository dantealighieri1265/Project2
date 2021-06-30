package utils;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaProperties {
    static ClassLoader loader = Thread.currentThread().getContextClassLoader();
    private static final String CONFIG = "kafka.properties";
    public static String TOPIC = "query";

    public static Properties getProducerProperties(String producerName){
        InputStream kafka_file = loader.getResourceAsStream(CONFIG);
        Properties props = new Properties();
        try {
            props.load(kafka_file);
        } catch (IOException e) {
            e.printStackTrace();
        }

        props.put(ProducerConfig.CLIENT_ID_CONFIG, producerName);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return props;

    }

    public static Properties getConsumerProperties(String consumerName){
        InputStream kafka_file = loader.getResourceAsStream(CONFIG);
        Properties props = new Properties();
        try {
            props.load(kafka_file);
        } catch (IOException e) {
            e.printStackTrace();
        }

        props.put(ConsumerConfig.GROUP_ID_CONFIG, "Consumer-Query2");
        // start reading from beginning of partition if no offset was created
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // exactly once semantic
        //props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, true);

        // key and value deserializers
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        return props;
    }

    public static void createTopic(final String topic,
                                   final Properties config) {
        final NewTopic newTopic = new NewTopic(topic, Optional.empty(), Optional.empty());
        try (final AdminClient adminClient = AdminClient.create(config)) {
            adminClient.createTopics(Collections.singletonList(newTopic)).all().get();
        } catch (final InterruptedException | ExecutionException e) {
            // Ignore if TopicExistsException, which may be valid if topic exists
            if (!(e.getCause() instanceof TopicExistsException)) {
                throw new RuntimeException(e);
            }
        }
    }
}
