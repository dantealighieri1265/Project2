package queries;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import queries.query1.Query1;
import queries.query2.Query2;
import queries.query3.Query3;
import utils.KafkaProperties;
import utils.Producer;
import utils.ShipData;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Properties;

public class QueriesStart {

    /*public static class Query1Thread extends Thread {

        public void run(DataStream<ShipData> dataStream){
            System.out.println("MyThread running");
            Query1.run(dataStream);
        }
    }

    public static class Query2Thread extends Thread {

        public void run(DataStream<ShipData> dataStream){
            System.out.println("MyThread running");
            Query2.run(dataStream);
        }
    }

    public static class Query3Thread extends Thread {

        public void run(DataStream<ShipData> dataStream){
            System.out.println("MyThread running");
            Query3.run(dataStream);
        }
    }*/

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties props = KafkaProperties.getConsumerProperties("Query2Consumer");

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(KafkaProperties.TOPIC,
                new SimpleStringSchema(), props);
        consumer.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofMillis(100)));

        DataStream<ShipData> dataStream = env
                .addSource(consumer)
                .map((MapFunction<String, ShipData>) s -> {
                    String[] values = s.split(",");
                    String dateString = values[7];
                    Long timestamp = null;
                    for (SimpleDateFormat dateFormat : Producer.dateFormats) {
                        try {
                            timestamp = dateFormat.parse(dateString).getTime();
                            //System.out.println("timestamp: "+new Date(timestamp));
                            break;
                        } catch (ParseException ignored) {
                        }
                    }
                    if (timestamp == null)
                        throw new NullPointerException();
                    return new ShipData(values[0], Integer.parseInt(values[1]), Double.parseDouble(values[3]),
                            Double.parseDouble(values[4]), timestamp, values[10]);
                }).filter((FilterFunction<ShipData>) shipData -> shipData.getLon() >= ShipData.getMinLon() &&
                        shipData.getLon() <= ShipData.getMaxLon() && shipData.getLat() >= ShipData.getMinLat() &&
                        shipData.getLat() <= ShipData.getMaxLat());

        Query1.run(dataStream);
        Query2.run(dataStream);
        Query3.run(dataStream);
        /*Query1Thread query1 = new Query1Thread();
        query1.start();
        Query2Thread query2 = new Query2Thread();
        query2.start();
        Query3Thread query3 = new Query3Thread();
        query3.start();*/

        try {
            env.execute("Queries");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
