package queries.query3;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import utils.ShipData;
import utils.SinkUtils;

import java.util.List;
import java.util.TreeMap;

public class Query3 {
    public static void run(DataStream<ShipData> dataStream){
        StreamingFileSink<String> sinkOneHour = SinkUtils.createStreamingFileSink(SinkUtils.QUERY3_OUTPUT_ONE_HOUR);
        StreamingFileSink<String> sinkTw0Hour = SinkUtils.createStreamingFileSink(SinkUtils.QUERY3_OUTPUT_TWO_HOUR);

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
