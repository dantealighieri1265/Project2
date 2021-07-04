package utils;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import queries.query1.Query1Result;
import queries.query2.Query2Result;
import queries.query3.Query3Result;

import java.io.IOException;
import java.io.InputStream;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class SinkUtils {

    public static final String QUERY1_OUTPUT_WEEKLY="Query1OutputWeekly";
    public static final String QUERY1_OUTPUT_MONTHLY="Query1OutputMonthly";
    public static final String QUERY2_OUTPUT_WEEKLY="Query2OutputWeekly";
    public static final String QUERY2_OUTPUT_MONTHLY="Query2OutputMonthly";
    public static final String QUERY3_OUTPUT_ONE_HOUR ="Query3OutputOneHour";
    public static final String QUERY3_OUTPUT_TWO_HOUR ="Query3OutputTwoHour";

    public static StreamingFileSink<String> createStreamingFileSink(String name){
        return StreamingFileSink
                .forRowFormat(new Path(name), new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
                                .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
                                .withMaxPartSize(1024*1024*1024)
                                .build())
                .build();
    }

    //todo mettere l'header ai csv

    //todo gestire gli zeri
    public static String createCSVQuery1(Query1Result query1Result){
        StringBuilder builder = new StringBuilder();
        builder.append(query1Result.getStartDate());
        builder.append(",");
        builder.append(query1Result.getCellId());
        long daysBetween = ChronoUnit.DAYS.between(query1Result.getStartDate(), query1Result.getEndDate());
        query1Result.getMap().forEach((k, v) -> {
            builder.append(",");
            builder.append(k);
            builder.append(",");
            builder.append((double)v/daysBetween);
        });
        return builder.toString();
    }


    public static String createCSVQuery2(List<TreeMap<Integer, List<Query2Result>>> list){
        StringBuilder builder = new StringBuilder();
        builder.append(list.get(0).firstEntry().getValue().get(0).getStartDate().toLocalDate());
        builder.append(",");
        builder.append("West Mediterranean");
        builder.append(",");
        builder.append("AM");
        builder.append(",");
        rankingBuilder(list, 0, builder);

        builder.append(",");
        builder.append("PM");
        builder.append(",");
        rankingBuilder(list, 1, builder);

        builder.append("\n");
        builder.append(list.get(0).firstEntry().getValue().get(0).getStartDate().toLocalDate());
        builder.append(",");
        builder.append("Est Mediterranean");
        builder.append(",");
        builder.append("AM");
        builder.append(",");
        rankingBuilder(list, 2, builder);

        builder.append(",");
        builder.append("PM");
        builder.append(",");
        rankingBuilder(list, 3, builder);

        return String.valueOf(builder);
    }

    private static void rankingBuilder(List<TreeMap<Integer, List<Query2Result>>> list, int index, StringBuilder builder){
        boolean atLest = false;
        int count = 0;
        for (int i=0; ;i++){
            if (count == 3)
                break;
            if (i >=  list.get(index).size()){
                break;
            }
            Optional<List<Query2Result>> first = list.get(index).values()
                    .stream()
                    .skip(i)
                    .findFirst();//valore associato al primo elemento della treemap

            if (first.isPresent()) {
                for (Query2Result result: first.get()){
                    if (count == 3)//se ho più di 3 elementi termino la classifica
                        break;
                    count++;
                    atLest = true;
                    String cellId = result.getCellId();
                    builder.append(cellId);
                    builder.append("-");
                }
            }
        }
        if (atLest)
            builder.deleteCharAt(builder.length()-1);
    }


    public static String createCSVQuery3(TreeMap<Double, List<Query3Result>> treeMap){
        StringBuilder builder = new StringBuilder();
        /*builder.append("timestamp");
        builder.append(",");
        builder.append("trip_1");
        builder.append(",");
        builder.append("rating_1");
        builder.append(",");
        builder.append("trip_2");
        builder.append(",");
        builder.append("rating_2");
        builder.append(",");
        builder.append("trip_3");
        builder.append(",");
        builder.append("rating_3");
        builder.append(",");
        builder.append("trip_4");
        builder.append(",");
        builder.append("rating_4");
        builder.append(",");
        builder.append("trip_5");
        builder.append(",");
        builder.append("rating_5");
        builder.append("\n");*/
        builder.append(treeMap.firstEntry().getValue().get(0).getStartDate());
        builder.append(",");
        int count = 0;
        for (int i=0;;i++){//Solo per scorrere le TreeMap
            if (count == 5)
                break;
            if (i >= treeMap.size()){
                break;
            }
            Optional<List<Query3Result>> first = treeMap.values()
                    .stream()
                    .skip(i)
                    .findFirst();
            if (first.isPresent()) {
                for (Query3Result result: first.get()){
                    if (count == 5)
                        break;
                    count++;
                    builder.append(result.getTripId());
                    builder.append(",");
                    builder.append(result.getDistance());
                    builder.append(",");
                }
            }
        }
        builder.deleteCharAt(builder.length()-1);
        return String.valueOf(builder);
    }
}
