package queries.query1;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
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
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.TimeUnit;

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
                }).filter((FilterFunction<ShipData>) shipData -> shipData.getLon() < ShipData.getLonSeparation());

        /*dataStream.keyBy(ShipData::getCell).window(TumblingEventTimeWindows.of(Time.milliseconds(6000))).map(new MapFunction<ShipData, String>() {
            @Override
            public String map(ShipData shipData) throws Exception {
                return shipData.getCell();
            }
        }).print();*/
        final StreamingFileSink<String> sinkWeekly = StreamingFileSink
                .forRowFormat(new Path("outputWeekly"), new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
                                .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
                                .withMaxPartSize(1024*10)
                                .build())
                .build();
        final StreamingFileSink<String> sinkMonthly = StreamingFileSink
                .forRowFormat(new Path("outputMonthly"), new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
                                .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
                                .withMaxPartSize(1024*10)
                                .build())
                .build();

        dataStream.keyBy(ShipData::getCell).window(TumblingEventTimeWindows.of(Time.days(7)))
                .aggregate(new Query1Aggregator(), new Query1Process())
        .map(new MapFunction<Query1Result, String>() {
            @Override
            public String map(Query1Result query1Result) throws Exception {
                StringBuilder builder = new StringBuilder();
                builder.append(query1Result.getStartDate())
                        .append(",").append(query1Result.getCellId());
                long daysBetween = ChronoUnit.DAYS.between(query1Result.getStartDate(), query1Result.getEndDate());
                String result = "";
                query1Result.getMap().forEach((k, v) -> {
                    builder.append(",").append(k).append(",").append((double)v/daysBetween);
                });
                return builder.toString();
            }
        }).addSink(sinkWeekly);

        dataStream.keyBy(ShipData::getCell).window(TumblingEventTimeWindows.of(Time.days(30)))
                .aggregate(new Query1Aggregator(), new Query1Process())
                .map((MapFunction<Query1Result, String>) query1Result -> {
                    StringBuilder builder = new StringBuilder();
                    builder.append(query1Result.getStartDate())
                            .append(",").append(query1Result.getCellId());
                    long daysBetween = ChronoUnit.DAYS.between(query1Result.getStartDate(), query1Result.getEndDate());
                    String result = "";
                    query1Result.getMap().forEach((k, v) -> {
                        builder.append(",").append(k).append(",").append(String.format(Locale.ENGLISH, "%.2g",(double)v/daysBetween));
                    });
                    return builder.toString();
                }).addSink(sinkMonthly);

        /*DataStream<String> ds = dataStream.keyBy(ShipData::getCell)
                .window(TumblingEventTimeWindows.of(Time.milliseconds(6000), Time.milliseconds(5999)))
                .aggregate(new Query1Aggregator());

        ds.print();*/

        try {
            env.execute("Query1");
        } catch (Exception e) {
            e.printStackTrace();
        }


    }

}
