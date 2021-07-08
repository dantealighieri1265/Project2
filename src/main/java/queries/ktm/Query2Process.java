package queries.ktm;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.TreeMap;
import java.util.HashMap;


public class Query2Process
        extends ProcessWindowFunction<Query2Result, Query2Result, String, TimeWindow>{
    HashMap<String, Long> hashMapAM = new HashMap<>();
    HashMap<String, Long> hashMapPM = new HashMap<>();
    TreeMap<Long, List<String>> treeMapAM = new TreeMap<>(Collections.reverseOrder());
    TreeMap<Long, List<String>> treeMapPM = new TreeMap<>(Collections.reverseOrder());
    /**
     * Recupera timestamp di apertura e chiusura della finestra e la chiave e li inserisce nell'output
     * @param key chiave del raggruppamento byKey
     * @param context contesto
     * @param iterable elementi nella finestra
     * @param collector risultati in uscita
     */
    @Override
    public void process(String key, Context context, Iterable<Query2Result> iterable, Collector<Query2Result> collector) {
        Query2Result query2Result = iterable.iterator().next();
        query2Result.setStartDate(Instant.ofEpochMilli(context.window().getStart()).atZone(ZoneId.systemDefault()).toLocalDateTime());
        query2Result.setEndDate(Instant.ofEpochMilli(context.window().getEnd()).atZone(ZoneId.systemDefault()).toLocalDateTime());
        query2Result.setSea(key);


        query2Result.getMapAM().forEach((key1, value) -> {
            Long count = hashMapAM.computeIfAbsent(key1.f0, val -> 0L);
            hashMapAM.put(key1.f0, count + value);
        });
        query2Result.getMapPM().forEach((key1, value) -> {
            Long count = hashMapPM.computeIfAbsent(key1.f0, val -> 0L);
            hashMapPM.put(key1.f0, count + value);
        });


        hashMapAM.forEach((key1, value) -> {
            List<String> list = treeMapAM.computeIfAbsent(value, k -> new ArrayList<>());
            if (!list.contains(key1))
                list.add(key1);
        });
        hashMapPM.forEach((key1, value) -> {
            List<String> list = treeMapPM.computeIfAbsent(value, k -> new ArrayList<>());
            if (!list.contains(key1))
                list.add(key1);
        });



        query2Result.setTreeMapAM(treeMapAM);
        query2Result.setTreeMapPM(treeMapPM);

        System.out.println(query2Result);
        collector.collect(query2Result);
    }
}
