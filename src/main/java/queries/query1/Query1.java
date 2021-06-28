package queries.query1;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.accumulators.AverageAccumulator;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import utils.Producer;
import utils.ShipData;

import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;

public class Query1 {
    static ClassLoader loader = Thread.currentThread().getContextClassLoader();

    public static class MyThread extends Thread {

        public void run(){
            System.out.println("MyThread running");
            Producer.main(null);
        }
    }

    public static void main(String[] args) {
        MyThread myThread = new MyThread();
        myThread.run();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        InputStream kafka_file = loader.getResourceAsStream("kafka.properties");
        Properties props = new Properties();

        props.put(ConsumerConfig.GROUP_ID_CONFIG, "Consumer-Query1");
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
        Producer.createTopic(Producer.TOPIC1, props);
        //props.setProperty("bootstrap.servers", "localhost:9092");

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(Producer.TOPIC1, new SimpleStringSchema(), props);
        consumer.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofMinutes(1)));

        DataStream<ShipData> dataStream = env
                .addSource(consumer)
                .map((MapFunction<String, ShipData>) s -> {
                    String[] values = s.split(",");
                    String dateString = values[7];
                    Long timestamp = null;
                    for (SimpleDateFormat dateFormat: Producer.dateFormats) {
                        try {
                            timestamp = dateFormat.parse(dateString).getTime();
                            break;
                        } catch (ParseException ignored) { }
                    }

                    return new ShipData(values[0], Integer.parseInt(values[1]), Double.parseDouble(values[3]),
                            Double.parseDouble(values[4]), timestamp, values[10]);
                });

        dataStream.map(new MapFunction<ShipData, Double>() {
            @Override
            public Double map(ShipData shipData) throws Exception {
                return shipData.getLat();
            }
        });

        dataStream.windowAll(TumblingEventTimeWindows.of(Time.milliseconds(10), Time.milliseconds(9))).
                aggregate(new Query1Aggregator()).print();

        try {
            env.execute("Query1");
        } catch (Exception e) {
            e.printStackTrace();
        }


    }

}
