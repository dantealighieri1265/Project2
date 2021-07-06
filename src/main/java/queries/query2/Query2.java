package queries.query2;

import benchmarks.BenchmarkMap;
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
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;
import utils.KafkaProperties;
import utils.Producer;
import utils.ShipData;
import utils.SinkUtils;

import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;

public class Query2 {

    public static void run(DataStream<ShipData> dataStream){
        StreamingFileSink<String> sinkWeekly = SinkUtils.createStreamingFileSink(SinkUtils.QUERY2_OUTPUT_WEEKLY);
        StreamingFileSink<String> sinkMonthly = SinkUtils.createStreamingFileSink(SinkUtils.QUERY2_OUTPUT_MONTHLY);
        StreamingFileSink<String> sinkWeeklyMetrics = SinkUtils.createStreamingFileSink(SinkUtils.QUERY2_OUTPUT_WEEKLY_BENCHMARK);
        StreamingFileSink<String> sinkMonthlyMetrics = SinkUtils.createStreamingFileSink(SinkUtils.QUERY2_OUTPUT_MONTHLY_BENCHMARK);

        //todo anziché raggruppare solo per cellId si potrebbe successivamente raggruppare anche per [sea,time]
        // il problema è che dopo la prima aggreagte i due valori non sono presenti. Bisognerebbe aggiungere
        // tali parametri agli attributi di Query2Result

        //todo si potrebbbe raggruppare direttamente per sea all'inizio
        DataStream<String> dataStreamWeeklyOutput=dataStream.keyBy(ShipData::getCell).window(TumblingEventTimeWindows.of(Time.days(7))).
                aggregate(new Query2Aggregator(), new Query2Process()).
                windowAll(TumblingEventTimeWindows.of(Time.days(7))).process(new Query2SortProcess()).
                map((MapFunction<List<TreeMap<Integer, List<Query2Result>>>, String>) SinkUtils::createCSVQuery2);

        Properties props = KafkaProperties.getFlinkProducerProperties("query2_output_producer");
        dataStreamWeeklyOutput.addSink(new FlinkKafkaProducer<>(KafkaProperties.QUERY2_WEEKLY_TOPIC,
                (KafkaSerializationSchema<String>) (s, aLong) ->
                        new ProducerRecord<>(KafkaProperties.QUERY2_WEEKLY_TOPIC, s.getBytes(StandardCharsets.UTF_8)),
                props, FlinkKafkaProducer.Semantic.EXACTLY_ONCE)).name("q2_weekly_kafka");
        dataStreamWeeklyOutput.addSink(sinkWeekly).name("q2_weekly").setParallelism(1);
        dataStreamWeeklyOutput.map(new BenchmarkMap()).addSink(sinkWeeklyMetrics).name("q2_weekly_bench").setParallelism(1);


        DataStream<String> dataStreamMonthlyOutput=dataStream.keyBy(ShipData::getCell).window(TumblingEventTimeWindows.of(Time.days(30))).
                aggregate(new Query2Aggregator(), new Query2Process()).
                windowAll(TumblingEventTimeWindows.of(Time.days(30))).process(new Query2SortProcess()).
                map((MapFunction<List<TreeMap<Integer, List<Query2Result>>>, String>) SinkUtils::createCSVQuery2);

        dataStreamMonthlyOutput.addSink(new FlinkKafkaProducer<>(KafkaProperties.QUERY2_MONTHLY_TOPIC,
                (KafkaSerializationSchema<String>) (s, aLong) ->
                        new ProducerRecord<>(KafkaProperties.QUERY2_MONTHLY_TOPIC, s.getBytes(StandardCharsets.UTF_8)),
                props, FlinkKafkaProducer.Semantic.EXACTLY_ONCE)).name("q2_monthly_kafka");
        dataStreamMonthlyOutput.addSink(sinkMonthly).name("q2_monthly").setParallelism(1);
        dataStreamMonthlyOutput.map(new BenchmarkMap()).addSink(sinkMonthlyMetrics).name("q2_monthly_bench").setParallelism(1);
    }


}





