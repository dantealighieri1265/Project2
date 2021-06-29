package queries.query2;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import queries.query1.Query1Result;
import utils.ShipData;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;


public class Query2Process
        extends ProcessWindowFunction<Query2Result, Query2Result, String, TimeWindow>{


    @Override
    public void process(String key, Context context, Iterable<Query2Result> iterable, Collector<Query2Result> collector) throws Exception {
        DateTimeFormatter formatters = DateTimeFormatter.ofPattern("yyyy/MM/dd hh:mm");
        Query2Result query2Result = iterable.iterator().next();
        //query1Outcome.setStartDate(new LocalDate(context.window().getStart()));
        query2Result.setStartDate(Instant.ofEpochMilli(context.window().getStart()).atZone(ZoneId.systemDefault()).toLocalDateTime());
        //query1Outcome.setEndDate(new LocalDate(context.window().getEnd()));
        query2Result.setEndDate(Instant.ofEpochMilli(context.window().getEnd()).atZone(ZoneId.systemDefault()).toLocalDateTime());
        //System.out.println(new Date(context.window().getStart()));
        query2Result.setCellId(key);
        collector.collect(query2Result);
    }
}
