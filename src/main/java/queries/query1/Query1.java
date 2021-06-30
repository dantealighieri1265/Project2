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
import utils.KafkaProperties;
import utils.Producer;
import utils.ShipData;
import utils.SinkUtils;

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
        myThread.start();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties props = KafkaProperties.getConsumerProperties("Query1Consumer");

        KafkaProperties.createTopic(KafkaProperties.TOPIC, props);
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(KafkaProperties.TOPIC,
                new SimpleStringSchema(), props);
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
                    if (timestamp == null)
                        throw new NullPointerException();
                    return new ShipData(values[0], Integer.parseInt(values[1]), Double.parseDouble(values[3]),
                            Double.parseDouble(values[4]), timestamp, values[10]);
                }).filter((FilterFunction<ShipData>) shipData -> shipData.getLon() >= ShipData.getMinLon() &&
                        shipData.getLon() <= ShipData.getMaxLon() && shipData.getLat() >= ShipData.getMinLat() &&
                        shipData.getLat() <= ShipData.getMaxLat())
                .filter((FilterFunction<ShipData>) shipData -> shipData.getLon() < ShipData.getLonSeparation());


        StreamingFileSink<String> sinkWeekly = SinkUtils.createStreamingFileSink("Query1OutputWeekly");
        StreamingFileSink<String> sinkMonthly = SinkUtils.createStreamingFileSink("Query1OutputMonthly");

        dataStream.keyBy(ShipData::getCell).window(TumblingEventTimeWindows.of(Time.days(7)))
                .aggregate(new Query1Aggregator(), new Query1Process())
        .map(SinkUtils::createCSVQuery1).addSink(sinkWeekly).setParallelism(1);

        dataStream.keyBy(ShipData::getCell).window(TumblingEventTimeWindows.of(Time.days(30)))
                .aggregate(new Query1Aggregator(), new Query1Process())
                .map(SinkUtils::createCSVQuery1).addSink(sinkMonthly).setParallelism(1);

        try {
            env.execute("Query1");
        } catch (Exception e) {
            e.printStackTrace();
        }


    }

}
