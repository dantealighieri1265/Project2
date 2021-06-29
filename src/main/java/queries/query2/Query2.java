package queries.query2;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import queries.query1.Query1;
import utils.Producer;
import utils.ShipData;

import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Properties;

public class Query2 {

    static ClassLoader loader = Thread.currentThread().getContextClassLoader();

    public static class MyThread extends Thread {

        public void run(){
            System.out.println("MyThread running");
            Producer.main(null);
        }
    }

    public static void main(String[] args) {
        Query1.MyThread myThread = new Query1.MyThread();
        myThread.run();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        InputStream kafka_file = loader.getResourceAsStream("kafka.properties");
        Properties props = new Properties();

        props.put(ConsumerConfig.GROUP_ID_CONFIG, "Consumer-Query2");
        // start reading from beginning of partition if no offset was created
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // exactly once semantic
        //props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, true);

        // key and value deserializers
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        try {
            props.load(kafka_file);
        } catch (IOException e) {
            e.printStackTrace();
        }
        Producer.createTopic(Producer.TOPIC, props);
        //props.setProperty("bootstrap.servers", "localhost:9092");

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(Producer.TOPIC, new SimpleStringSchema(), props);
        consumer.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofMillis(100)));

        DataStream<ShipData> dataStream = env
                .addSource(consumer)
                .map((MapFunction<String, ShipData>) s -> {
                    String[] values = s.split(",");
                    String dateString = values[7];
                    Long timestamp = null;
                    for (SimpleDateFormat dateFormat: Producer.dateFormats) {
                        try {
                            timestamp = dateFormat.parse(dateString).getTime();
                            //System.out.println("timestamp: "+new Date(timestamp));
                            break;
                        } catch (ParseException ignored) { }
                    }
                    return new ShipData(values[0], Integer.parseInt(values[1]), Double.parseDouble(values[3]),
                            Double.parseDouble(values[4]), timestamp, values[10]);
                });

        dataStream.keyBy(ShipData::getCell).window(TumblingEventTimeWindows.of(Time.days(7))).
                aggregate(new Query2Aggregator(), new Query2Process()).print();

        try {
            env.execute("Query2");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
