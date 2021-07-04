package queries.query1;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;
import utils.KafkaProperties;
import utils.ShipData;
import utils.SinkUtils;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

public class Query1 {

    public static void run(DataStream<ShipData> dataStreamNoFilter){

        DataStream<ShipData> dataStream = dataStreamNoFilter
                .filter((FilterFunction<ShipData>) shipData -> shipData.getLon() < ShipData.getLonSeparation());


        StreamingFileSink<String> sinkWeekly = SinkUtils.createStreamingFileSink(SinkUtils.QUERY1_OUTPUT_WEEKLY);
        StreamingFileSink<String> sinkMonthly = SinkUtils.createStreamingFileSink(SinkUtils.QUERY1_OUTPUT_MONTHLY);

        DataStream<String> dataStreamWeeklyOutput=dataStream.keyBy(ShipData::getCell).window(TumblingEventTimeWindows.of(Time.days(7)))
                .aggregate(new Query1Aggregator(), new Query1Process())
                .map(SinkUtils::createCSVQuery1);

        Properties props = KafkaProperties.getFlinkProducerProperties("query1_output_producer");
        dataStreamWeeklyOutput.addSink(new FlinkKafkaProducer<>(KafkaProperties.QUERY1_WEEKLY_TOPIC,
                (KafkaSerializationSchema<String>) (s, aLong) ->
                        new ProducerRecord<>(KafkaProperties.QUERY1_WEEKLY_TOPIC, s.getBytes(StandardCharsets.UTF_8)),
                props, FlinkKafkaProducer.Semantic.EXACTLY_ONCE));
        dataStreamWeeklyOutput.addSink(sinkWeekly).setParallelism(1);


        DataStream<String> dataStreamMonthlyOutput=dataStream.keyBy(ShipData::getCell).window(TumblingEventTimeWindows.of(Time.days(30)))
                .aggregate(new Query1Aggregator(), new Query1Process())
                .map(SinkUtils::createCSVQuery1);

        dataStreamMonthlyOutput.addSink(new FlinkKafkaProducer<>(KafkaProperties.QUERY1_MONTHLY_TOPIC,
                (KafkaSerializationSchema<String>) (s, aLong) ->
                        new ProducerRecord<>(KafkaProperties.QUERY1_MONTHLY_TOPIC, s.getBytes(StandardCharsets.UTF_8)),
                props, FlinkKafkaProducer.Semantic.EXACTLY_ONCE));
        dataStreamMonthlyOutput.addSink(sinkMonthly).setParallelism(1);



    }

}
