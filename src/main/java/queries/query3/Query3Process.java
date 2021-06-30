package queries.query3;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import queries.query2.Query2Result;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;


public class Query3Process
        extends ProcessWindowFunction<Query3Result, Query3Result, String, TimeWindow>{


    @Override
    public void process(String key, Context context, Iterable<Query3Result> iterable, Collector<Query3Result> collector) throws Exception {
        DateTimeFormatter formatters = DateTimeFormatter.ofPattern("yyyy/MM/dd hh:mm");
        Query3Result query3Result = iterable.iterator().next();
        //query1Outcome.setStartDate(new LocalDate(context.window().getStart()));
        query3Result.setStartDate(Instant.ofEpochMilli(context.window().getStart()).atZone(ZoneId.systemDefault()).toLocalDateTime());
        //query1Outcome.setEndDate(new LocalDate(context.window().getEnd()));
        query3Result.setEndDate(Instant.ofEpochMilli(context.window().getEnd()).atZone(ZoneId.systemDefault()).toLocalDateTime());
        //System.out.println("PROCESS: "+(context.window().getStart())+", "+Instant.ofEpochMilli(context.window().getStart()).atZone(ZoneId.systemDefault()).toLocalDateTime());
        query3Result.setTripId(key);
        collector.collect(query3Result);
    }
}
