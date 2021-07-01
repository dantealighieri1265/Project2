package queries.query3;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import queries.query1.Query1;
import queries.query2.Query2Aggregator;
import queries.query2.Query2Process;
import queries.query2.Query2Result;
import queries.query2.Query2SortProcess;
import utils.KafkaProperties;
import utils.Producer;
import utils.ShipData;
import utils.SinkUtils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.Properties;
import java.util.TreeMap;

public class Query3 {
    public static void run(DataStream<ShipData> dataStream){
        StreamingFileSink<String> sinkOneHour = SinkUtils.createStreamingFileSink("Query3OutputOneHour");
        StreamingFileSink<String> sinkTw0Hour = SinkUtils.createStreamingFileSink("Query3OutputTwoHour");

        dataStream.keyBy(ShipData::getTripId).window(TumblingEventTimeWindows.of(Time.hours(1))).
                aggregate(new Query3Aggregator(), new Query3Process()).
                windowAll(TumblingEventTimeWindows.of(Time.hours(1))).process(new Query3SortProcess()).
                map((MapFunction<TreeMap<Double, List<Query3Result>>, String>) SinkUtils::createCSVQuery3).
                addSink(sinkOneHour).setParallelism(1);

        dataStream.keyBy(ShipData::getTripId).window(TumblingEventTimeWindows.of(Time.hours(2))).
                aggregate(new Query3Aggregator(), new Query3Process()).
                windowAll(TumblingEventTimeWindows.of(Time.hours(2))).process(new Query3SortProcess()).
                map((MapFunction<TreeMap<Double, List<Query3Result>>, String>) SinkUtils::createCSVQuery3).
                addSink(sinkTw0Hour).setParallelism(1);
    }

}
