package queries.query1;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;


public class Query1Process
        extends ProcessWindowFunction<Query1Result, Query1Result, String, TimeWindow> {
    /**
     * Recupera timestamp di apertura e chiusura della finestra e la chiave e li inserisce nell'output
     * @param key chiave del raggruppamento byKey
     * @param context contesto
     * @param iterable elementi nella finestra
     * @param collector risultati in uscita
     */
    @Override
    public void process(String key, Context context, Iterable<Query1Result> iterable, Collector<Query1Result> collector) {
        Query1Result query1Result = iterable.iterator().next();
        //query1Outcome.setStartDate(new LocalDate(context.window().getStart()));
        query1Result.setStartDate(Instant.ofEpochMilli(context.window().getStart()).atZone(ZoneId.systemDefault()).toLocalDateTime());
        //query1Outcome.setEndDate(new LocalDate(context.window().getEnd()));
        query1Result.setEndDate(Instant.ofEpochMilli(context.window().getEnd()).atZone(ZoneId.systemDefault()).toLocalDateTime());
        //System.out.println(new Date(context.window().getStart()));
        query1Result.setCellId(key);
        collector.collect(query1Result);
    }

    public static void main(String[] args) {
        System.out.println(Instant.ofEpochMilli(1431656219899L).atZone(ZoneId.systemDefault()).toLocalDateTime());
    }
}
