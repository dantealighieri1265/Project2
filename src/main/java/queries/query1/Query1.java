package queries.query1;

import benchmarks.BenchmarkMap;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper;
import org.apache.flink.metrics.Meter;
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
import java.util.Properties;

public class Query1 {


    private static class MyMapper extends RichMapFunction<Query1Result, Query1Result> {

        private transient Meter meter;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            this.meter = getRuntimeContext()
                    .getMetricGroup()
                    .meter("myMeter", new DropwizardMeterWrapper(new com.codahale.metrics.Meter()));
        }

        @Override
        public Query1Result map(Query1Result value) throws Exception {
            this.meter.markEvent();
            return value;
        }
    }

    /**
     *
     * @param dataStreamNoFilter DataStream in ingresso su cui eseguire il processamento
     */
    public static void run(DataStream<ShipData> dataStreamNoFilter){

        DataStream<ShipData> dataStream = dataStreamNoFilter //filtraggio Mediterraneo Occidentale
                .filter((FilterFunction<ShipData>) shipData -> shipData.getLon() < ShipData.getLonSeaSeparation());

        // Definizione sink
        StreamingFileSink<String> sinkWeekly = SinkUtils.createStreamingFileSink(SinkUtils.QUERY1_OUTPUT_WEEKLY);
        StreamingFileSink<String> sinkMonthly = SinkUtils.createStreamingFileSink(SinkUtils.QUERY1_OUTPUT_MONTHLY);
        StreamingFileSink<String> sinkWeeklyMetrics = SinkUtils.createStreamingFileSink(SinkUtils.QUERY1_OUTPUT_WEEKLY_BENCHMARK);
        StreamingFileSink<String> sinkMonthlyMetrics = SinkUtils.createStreamingFileSink(SinkUtils.QUERY1_OUTPUT_MONTHLY_BENCHMARK);

        //datastream per processamento settimanale
        DataStream<String> dataStreamWeeklyOutput=dataStream.keyBy(ShipData::getCell)
                .window(TumblingEventTimeWindows.of(Time.days(7), Time.minutes(3648)))
                .aggregate(new Query1Aggregator(), new Query1Process()) //accumulazione dei dati e conteggio per finestra
                .map(SinkUtils::createCSVQuery1); //calcolo media e generazione risultati

        //invio dei risultati su topic kafka
        Properties props = KafkaProperties.getFlinkProducerProperties("query1_output_producer");
        dataStreamWeeklyOutput.addSink(new FlinkKafkaProducer<>(KafkaProperties.QUERY1_WEEKLY_TOPIC,
                (KafkaSerializationSchema<String>) (s, aLong) ->
                        new ProducerRecord<>(KafkaProperties.QUERY1_WEEKLY_TOPIC, s.getBytes(StandardCharsets.UTF_8)),
                props, FlinkKafkaProducer.Semantic.EXACTLY_ONCE)).name("q1_weekly_kafka");
        //generazione dei file di output
        //dataStreamWeeklyOutput.addSink(sinkWeekly).name("q1_weekly").setParallelism(1);
        //dataStreamWeeklyOutput.addSink(new BenchmarkFlinkSink());
        //generazione benchmark
        //dataStreamWeeklyOutput.map(new BenchmarkMap()).addSink(sinkWeeklyMetrics).name("q1_weekly_bench").setParallelism(1);

        //datastream per processamento mensile
        DataStream<String> dataStreamMonthlyOutput=dataStream.keyBy(ShipData::getCell).window(TumblingEventTimeWindows.of(Time.days(28), Time.minutes(23808)))
                .aggregate(new Query1Aggregator(), new Query1Process()) //accumulazione dei dati e conteggio per finestra
                .map(SinkUtils::createCSVQuery1); //calcolo media e generazione risultati

        //invio dei risultati su topic kafka
        dataStreamMonthlyOutput.addSink(new FlinkKafkaProducer<>(KafkaProperties.QUERY1_MONTHLY_TOPIC,
                (KafkaSerializationSchema<String>) (s, aLong) ->
                        new ProducerRecord<>(KafkaProperties.QUERY1_MONTHLY_TOPIC, s.getBytes(StandardCharsets.UTF_8)),
                props, FlinkKafkaProducer.Semantic.EXACTLY_ONCE)).name("q1_monthly_kafka");
        //generazione dei file di output
        //dataStreamMonthlyOutput.addSink(sinkMonthly).name("q1_monthly").setParallelism(1);
        //generazione benchmark
        //dataStreamMonthlyOutput.map(new BenchmarkMap()).addSink(sinkMonthlyMetrics).name("q1_monthly_bench").setParallelism(1);
        //dataStreamMonthlyOutput.addSink(new BenchmarkFlinkSink());
    }

}
