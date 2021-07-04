package utils;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import queries.query1.Query1Result;
import queries.query2.Query2Result;
import queries.query3.Query3Result;

import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class SinkUtils {

    public static final String QUERY1_OUTPUT_WEEKLY="/home/marco/Scrivania/Project2/Query1OutputWeekly";
    public static final String QUERY1_OUTPUT_MONTHLY="/home/marco/Scrivania/Project2/Query1OutputMonthly";
    public static final String QUERY2_OUTPUT_WEEKLY="/home/marco/Scrivania/Project2/Query2OutputWeekly";
    public static final String QUERY2_OUTPUT_MONTHLY="/home/marco/Scrivania/Project2/Query2OutputMonthly";
    public static final String QUERY3_OUTPUT_ONE_HOUR ="/home/marco/Scrivania/Project2/Query3OutputOneHour";
    public static final String QUERY3_OUTPUT_TWO_HOUR ="/home/marco/Scrivania/Project2/Query3OutputTwoHour";

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

    public static String createCSVQuery2(List<TreeMap<Integer, List<Query2Result>>> list){
        StringBuilder builder = new StringBuilder();
        boolean atLest = false;
        builder.append(list.get(0).firstEntry().getValue().get(0).getStartDate().toLocalDate());
        builder.append(",");
        builder.append("West Mediterranean");
        builder.append(",");
        builder.append("AM");
        builder.append(",");
        int count = 0;
        for (int i=0; ;i++){
            if (count == 3)
                break;
            if (i >=  list.get(0).size()){
                break;
            }
            Optional<List<Query2Result>> first = list.get(0).values()
                    .stream()
                    .skip(i)
                    .findFirst();//valore associato al primo elemento della treemap

            //tree map --> Count:ListResultStessaCount
            if (first.isPresent()) {
                for (Query2Result result: first.get()){
                    //se ho più di 3 elementi termino la classifica
                    if (count == 3)
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
        atLest = false;


        builder.append(",");
        builder.append("PM");
        builder.append(",");
        for (int i=0; i<3;){
            Optional<List<Query2Result>> first = list.get(1).values()
                    .stream()
                    .skip(i)
                    .findFirst();
            if (first.isPresent()) {
                for (Query2Result result: first.get()){
                    //se ho più di 3 elementi termino la classifica
                    if (i == 3)
                        break;
                    i++;

                    atLest = true;
                    String cellId = result.getCellId();
                    builder.append(cellId);
                    builder.append("-");
                }
            }else {
                i++;
            }
        }
        if (atLest)
            builder.deleteCharAt(builder.length()-1);
        atLest = false;
        builder.append("\n");


        builder.append(list.get(0).firstEntry().getValue().get(0).getStartDate().toLocalDate());
        builder.append(",");
        builder.append("Est Mediterranean");
        builder.append(",");
        builder.append("AM");
        builder.append(",");
        count = 0;
        for (int i=0; ;i++){
            if (count == 3)
                break;
            if (i >=  list.get(0).size()){
                break;
            }
            Optional<List<Query2Result>> first = list.get(2).values()
                    .stream()
                    .skip(i)
                    .findFirst();//valore associato al primo elemento della treemap

            //tree map --> Count:ListResultStessaCount
            if (first.isPresent()) {
                for (Query2Result result: first.get()){
                    //se ho più di 3 elementi termino la classifica
                    if (count == 3)
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
        atLest = false;
        builder.append(",");
        builder.append("PM");
        builder.append(",");
        count = 0;
        for (int i=0; ;i++){
            if (count == 3)
                break;
            if (i >=  list.get(2).size()){
                break;
            }
            Optional<List<Query2Result>> first = list.get(3).values()
                    .stream()
                    .skip(i)
                    .findFirst();//valore associato al primo elemento della treemap

            //tree map --> Count:ListResultStessaCount
            if (first.isPresent()) {
                for (Query2Result result: first.get()){
                    //se ho più di 3 elementi termino la classifica
                    if (count == 3)
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

        return String.valueOf(builder);
    }
}
