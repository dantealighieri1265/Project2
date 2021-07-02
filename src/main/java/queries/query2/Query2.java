package queries.query2;

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
import utils.KafkaProperties;
import utils.Producer;
import utils.ShipData;
import utils.SinkUtils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;

public class Query2 {

    public static void run(DataStream<ShipData> dataStream){
        StreamingFileSink<String> sinkWeekly = SinkUtils.createStreamingFileSink(SinkUtils.QUERY2_OUTPUT_WEEKLY);
        StreamingFileSink<String> sinkMonthly = SinkUtils.createStreamingFileSink(SinkUtils.QUERY2_OUTPUT_MONTHLY);

        //todo anziché raggruppare solo per cellId si potrebbe successivamente raggruppare anche per [sea,time]
        // il problema è che dopo la prima aggreagte i due valori non sono presenti. Bisognerebbe aggiungere
        // tali parametri agli attributi di Query2Result

        //todo si potrebbbe raggruppare direttamente per sea all'inizio
        dataStream.keyBy(ShipData::getCell).window(TumblingEventTimeWindows.of(Time.days(7))).
                aggregate(new Query2Aggregator(), new Query2Process()).
                windowAll(TumblingEventTimeWindows.of(Time.days(7))).process(new Query2SortProcess()).
                map((MapFunction<List<TreeMap<Integer, List<Query2Result>>>, String>) SinkUtils::createCSVQuery2).
                addSink(sinkWeekly).setParallelism(1);


        dataStream.keyBy(ShipData::getCell).window(TumblingEventTimeWindows.of(Time.days(30))).
                aggregate(new Query2Aggregator(), new Query2Process()).
                windowAll(TumblingEventTimeWindows.of(Time.days(30))).process(new Query2SortProcess()).
                map((MapFunction<List<TreeMap<Integer, List<Query2Result>>>, String>) SinkUtils::createCSVQuery2).
                addSink(sinkMonthly).setParallelism(1);
    }


}





