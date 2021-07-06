package queries.query3;

import benchmarks.BenchmarkMap;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;
import utils.KafkaProperties;
import utils.ShipData;
import utils.SinkUtils;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Properties;
import java.util.TreeMap;

public class Query3 {
    public static void run(DataStream<ShipData> dataStream){
        StreamingFileSink<String> sinkOneHour = SinkUtils.createStreamingFileSink(SinkUtils.QUERY3_OUTPUT_ONE_HOUR);
        StreamingFileSink<String> sinkTw0Hour = SinkUtils.createStreamingFileSink(SinkUtils.QUERY3_OUTPUT_TWO_HOUR);
        StreamingFileSink<String> sinkOneHourMetrics = SinkUtils.createStreamingFileSink(SinkUtils.QUERY3_OUTPUT_ONE_HOUR_BENCHMARK);
        StreamingFileSink<String> sinkTwoHourMetrics = SinkUtils.createStreamingFileSink(SinkUtils.QUERY3_OUTPUT_TWO_HOUR_BENCHMARK);

        //todo capire bene cosa intende per tempo reale

        DataStream<String> dataStreamOneHourOutput=dataStream.keyBy(ShipData::getTripId).window(TumblingEventTimeWindows.of(Time.hours(1))).
                aggregate(new Query3Aggregator(), new Query3Process()).
                windowAll(TumblingEventTimeWindows.of(Time.hours(1))).process(new Query3SortProcess()).
                map((MapFunction<TreeMap<Double, List<Query3Result>>, String>) SinkUtils::createCSVQuery3);

        Properties props = KafkaProperties.getFlinkProducerProperties("query3_output_producer");
        dataStreamOneHourOutput.addSink(new FlinkKafkaProducer<>(KafkaProperties.QUERY3_ONE_HOUR_TOPIC,
                (KafkaSerializationSchema<String>) (s, aLong) ->
                        new ProducerRecord<>(KafkaProperties.QUERY3_ONE_HOUR_TOPIC, s.getBytes(StandardCharsets.UTF_8)),
                props, FlinkKafkaProducer.Semantic.EXACTLY_ONCE)).name("q3_one_hour_kafka");
        dataStreamOneHourOutput.addSink(sinkOneHour).name("q3_one_hour").setParallelism(1);
        dataStreamOneHourOutput.map(new BenchmarkMap()).addSink(sinkOneHourMetrics).name("q3_one_hour_bench").setParallelism(1);


        DataStream<String> dataStreamTwoHourOutput=dataStream.keyBy(ShipData::getTripId).window(TumblingEventTimeWindows.of(Time.hours(2))).
                aggregate(new Query3Aggregator(), new Query3Process()).
                windowAll(TumblingEventTimeWindows.of(Time.hours(2))).process(new Query3SortProcess()).
                map((MapFunction<TreeMap<Double, List<Query3Result>>, String>) SinkUtils::createCSVQuery3);

        dataStreamTwoHourOutput.addSink(new FlinkKafkaProducer<>(KafkaProperties.QUERY3_TWO_HOUR_TOPIC,
                (KafkaSerializationSchema<String>) (s, aLong) ->
                        new ProducerRecord<>(KafkaProperties.QUERY3_TWO_HOUR_TOPIC, s.getBytes(StandardCharsets.UTF_8)),
                props, FlinkKafkaProducer.Semantic.EXACTLY_ONCE)).name("q3_two_hour_kafka");
        dataStreamTwoHourOutput.addSink(sinkTw0Hour).name("q3_two_hour").setParallelism(1);
        dataStreamTwoHourOutput.map(new BenchmarkMap()).addSink(sinkTwoHourMetrics).name("q3_two_hour_bench").setParallelism(1);
    }

}
