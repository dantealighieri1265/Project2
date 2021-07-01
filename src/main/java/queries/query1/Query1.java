package queries.query1;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import utils.KafkaProperties;
import utils.Producer;
import utils.ShipData;
import utils.SinkUtils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class Query1 {

    public static void run(DataStream<ShipData> dataStreamNoFilter){

        DataStream<ShipData> dataStream = dataStreamNoFilter
                .filter((FilterFunction<ShipData>) shipData -> shipData.getLon() < ShipData.getLonSeparation());


        StreamingFileSink<String> sinkWeekly = SinkUtils.createStreamingFileSink("Query1OutputWeekly");
        StreamingFileSink<String> sinkMonthly = SinkUtils.createStreamingFileSink("Query1OutputMonthly");

        dataStream.keyBy(ShipData::getCell).window(TumblingEventTimeWindows.of(Time.days(7)))
                .aggregate(new Query1Aggregator(), new Query1Process())
                .map(SinkUtils::createCSVQuery1).addSink(sinkWeekly).setParallelism(1);

        dataStream.keyBy(ShipData::getCell).window(TumblingEventTimeWindows.of(Time.days(30)))
                .aggregate(new Query1Aggregator(), new Query1Process())
                .map(SinkUtils::createCSVQuery1).addSink(sinkMonthly).setParallelism(1);

    }

}
