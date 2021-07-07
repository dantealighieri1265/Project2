package queries.query2;

import benchmarks.BenchmarkFlinkSink;
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

public class Query2 {

    public static void run(DataStream<ShipData> dataStream){
        // Definizione sink
        StreamingFileSink<String> sinkWeekly = SinkUtils.createStreamingFileSink(SinkUtils.QUERY2_OUTPUT_WEEKLY);
        StreamingFileSink<String> sinkMonthly = SinkUtils.createStreamingFileSink(SinkUtils.QUERY2_OUTPUT_MONTHLY);
        StreamingFileSink<String> sinkWeeklyMetrics = SinkUtils.createStreamingFileSink(SinkUtils.QUERY2_OUTPUT_WEEKLY_BENCHMARK);
        StreamingFileSink<String> sinkMonthlyMetrics = SinkUtils.createStreamingFileSink(SinkUtils.QUERY2_OUTPUT_MONTHLY_BENCHMARK);

        //todo anziché raggruppare solo per cellId si potrebbe successivamente raggruppare anche per [sea,time]
        // il problema è che dopo la prima aggreagte i due valori non sono presenti. Bisognerebbe aggiungere
        // tali parametri agli attributi di Query2Result

        //todo si potrebbbe raggruppare direttamente per sea all'inizio
        //datastream per processamento settimanale
        DataStream<String> dataStreamWeeklyOutput=dataStream.keyBy(ShipData::getCell).window(TumblingEventTimeWindows.of(Time.days(7), Time.minutes(3648))).
                aggregate(new Query2Aggregator(), new Query2Process()).
                windowAll(TumblingEventTimeWindows.of(Time.days(7))).process(new Query2SortProcess()).
                map((MapFunction<List<TreeMap<Integer, List<Query2Result>>>, String>) SinkUtils::createCSVQuery2);

        Properties props = KafkaProperties.getFlinkProducerProperties("query2_output_producer");
        //invio dei risultati su topic kafka
        dataStreamWeeklyOutput.addSink(new FlinkKafkaProducer<>(KafkaProperties.QUERY2_WEEKLY_TOPIC,
                (KafkaSerializationSchema<String>) (s, aLong) ->
                        new ProducerRecord<>(KafkaProperties.QUERY2_WEEKLY_TOPIC, s.getBytes(StandardCharsets.UTF_8)),
                props, FlinkKafkaProducer.Semantic.EXACTLY_ONCE)).name("q2_weekly_kafka");
        //generazione dei file di output
        //dataStreamWeeklyOutput.addSink(sinkWeekly).name("q2_weekly").setParallelism(1);
        //generazione benchmark
        //dataStreamWeeklyOutput.map(new BenchmarkMap()).addSink(sinkWeeklyMetrics).name("q2_weekly_bench").setParallelism(1);
        //dataStreamWeeklyOutput.addSink(new BenchmarkFlinkSink());

        //datastream per processamento mensile
        DataStream<String> dataStreamMonthlyOutput=dataStream.keyBy(ShipData::getCell).window(TumblingEventTimeWindows.of(Time.days(28), Time.minutes(23808))).
                aggregate(new Query2Aggregator(), new Query2Process()).
                windowAll(TumblingEventTimeWindows.of(Time.days(30))).process(new Query2SortProcess()).
                map((MapFunction<List<TreeMap<Integer, List<Query2Result>>>, String>) SinkUtils::createCSVQuery2);

        //invio dei risultati su topic kafka
        dataStreamMonthlyOutput.addSink(new FlinkKafkaProducer<>(KafkaProperties.QUERY2_MONTHLY_TOPIC,
                (KafkaSerializationSchema<String>) (s, aLong) ->
                        new ProducerRecord<>(KafkaProperties.QUERY2_MONTHLY_TOPIC, s.getBytes(StandardCharsets.UTF_8)),
                props, FlinkKafkaProducer.Semantic.EXACTLY_ONCE)).name("q2_monthly_kafka");
        //generazione dei file di output
        //dataStreamMonthlyOutput.addSink(sinkMonthly).name("q2_monthly").setParallelism(1);
        //generazione benchmark
        //dataStreamMonthlyOutput.map(new BenchmarkMap()).addSink(sinkMonthlyMetrics).name("q2_monthly_bench").setParallelism(1);
        //dataStreamMonthlyOutput.addSink(new BenchmarkFlinkSink());
    }


}





