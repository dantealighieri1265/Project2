package utils;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ExecutionException;


public class Producer {
    private static final String COMMA_DELIMITER = ",";
    public static String TOPIC1 = "query1";
    private static long TOTAL_MILL_TIME = 1 * 10 * 1000;

    public static final SimpleDateFormat[] dateFormats = {new SimpleDateFormat("dd/MM/yy HH:mm"),
            new SimpleDateFormat("dd-MM-yy HH:mm")};
    private static final String FILE_NAME = "prj2_dataset.csv";
    static ClassLoader loader = Thread.currentThread().getContextClassLoader();


    public static void main(String[] args) {
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        Instant start = Instant.now();
        TreeMap<Long, List<String>> records = retrieve_file();
        System.out.println(records.values().size());
        System.out.println(new Date(records.firstKey())+", "+new Date(records.lastKey()));
        System.out.println(records.firstKey() +", "+records.lastKey()+", "+(records.lastKey()-records.firstKey())/1000/60/60/24);
        System.out.println(records.firstKey() +", "+records.lastKey()+", "+(records.lastKey()-records.firstKey()));
        /*for (List<String> l: records.values()){
            System.out.println(l);
        }*/
        kafka_injector(records);
        Instant end = Instant.now();
        System.out.println("Query 1 completed in " + Duration.between(start, end).toMillis() + "ms");
    }

    public static TreeMap<Long, List<String>> retrieve_file(){
        TreeMap<Long, List<String>> records = new TreeMap<>();
        try (BufferedReader br = new BufferedReader(new FileReader(FILE_NAME))) {
            String line;
            boolean header = true;
            while ((line = br.readLine()) != null) {
                if (header){
                    header = false;
                    continue;
                }
                String[] values = line.split(COMMA_DELIMITER);
                String timestamp = values[7];
                Long long_timestamp = null;
                
                for (SimpleDateFormat dateFormat: dateFormats) {
                    try {
                        long_timestamp = dateFormat.parse(timestamp).getTime();
                        break;
                    } catch (ParseException ignored) { }
                }
                List<String> record_values = records.computeIfAbsent(long_timestamp, k -> new ArrayList<>());
                record_values.add(line);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return records;
    }

    public static void kafka_injector(TreeMap<Long, List<String>> records){
        InputStream kafka_file = loader.getResourceAsStream("kafka.properties");
        Properties props = new Properties();
        try {
            props.load(kafka_file);
        } catch (IOException e) {
            e.printStackTrace();
        }
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "producer-query1");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        org.apache.kafka.clients.producer.Producer<Long, String> producer = new KafkaProducer<>(props);
        Long key_prev = null;
        int line = 0;
        double time_unit = (double) TOTAL_MILL_TIME / (double)(records.lastKey() - records.firstKey());
        for (Map.Entry<Long, List<String>> entry : records.entrySet()) {
            List<String> value = entry.getValue();
            Long key = entry.getKey();
            long sleep = 0;
            if (key_prev != null) {
                 sleep = (long) ((key - key_prev) * time_unit);
                try {
                    Thread.sleep(sleep);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            for (String val: value){
                line++;
                long finalSleep = sleep;
                int finalLine = line;
                producer.send(new ProducerRecord<>(TOPIC1, key, val), (m, e) -> {
                    if (e != null) {
                        e.printStackTrace();
                    } else {
                        /*System.out.printf("line: "+ finalLine +" sleep: "+ finalSleep+" Key: "+key+" Value: "+value +" Produced record to topic " +
                                "%s partition [%d] @ offset %d%n", m.topic(), m.partition(), m.offset());*/
                    }
                });
            }
            key_prev = key;
        }
        //producer.close(Duration.ofMinutes(6));
        producer.flush();
    }

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
}