package utils;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import queries.query1.Query1Result;
import queries.query2.Query2Result;
import queries.query3.Query3Result;

import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

public class SinkUtils {

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
        for (int i=0; i<5; i++){
            Optional<List<Query3Result>> first = treeMap.values()
                    .stream()
                    .skip(i)
                    .findFirst();
            if (first.isPresent()) {
                for (Query3Result result: first.get()){
                    i++;
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

    public static String createCSVQuery2(List<TreeMap<String, Query2Result>> list){
        StringBuilder builder = new StringBuilder();
        boolean atLest = false;
        builder.append(list.get(0).firstEntry().getValue().getStartDate().toLocalDate());
        builder.append(",");
        builder.append("West Mediterranean");
        builder.append(",");
        builder.append("AM");
        builder.append(",");
        for (int i=0; i<3; i++){
            Optional<Query2Result> first = list.get(0).values()
                    .stream()
                    .skip(i)
                    .findFirst();
            if (first.isPresent() && first.get().getCountWestAM()!=0) {
                atLest = true;
                String cellId = first.get().getCellId();
                builder.append(cellId);
                builder.append("-");
            }

        }
        if (atLest)
            builder.deleteCharAt(builder.length()-1);
        atLest = false;

        builder.append(",");
        builder.append("PM");
        builder.append(",");
        for (int i=0; i<3; i++){
            Optional<Query2Result> first = list.get(1).values()
                    .stream()
                    .skip(i)
                    .findFirst();
            if (first.isPresent() && first.get().getCountWestPM()!=0) {
                atLest = true;
                String cellId = first.get().getCellId();
                builder.append(cellId);
                builder.append("-");
            }
        }
        if (atLest)
            builder.deleteCharAt(builder.length()-1);
        atLest = false;
        builder.append("\n");


        builder.append(list.get(0).firstEntry().getValue().getStartDate().toLocalDate());
        builder.append(",");
        builder.append("Est Mediterranean");
        builder.append(",");
        builder.append("AM");
        builder.append(",");
        for (int i=0; i<3; i++){
            Optional<Query2Result> first = list.get(2).values()
                    .stream()
                    .skip(i)
                    .findFirst();
            if (first.isPresent() && first.get().getCountEstAM()!=0) {
                atLest = true;
                String cellId = first.get().getCellId();
                builder.append(cellId);
                builder.append("-");
            }
        }
        if (atLest)
            builder.deleteCharAt(builder.length()-1);
        atLest = false;
        builder.append(",");
        builder.append("PM");
        builder.append(",");
        for (int i=0; i<3; i++){
            Optional<Query2Result> first = list.get(3).values()
                    .stream()
                    .skip(i)
                    .findFirst();
            if (first.isPresent() && first.get().getCountEstPM()!=0 ) {
                atLest = true;
                String cellId = first.get().getCellId();
                builder.append(cellId);
                builder.append("-");
            }
        }
        if (atLest)
            builder.deleteCharAt(builder.length()-1);
        atLest = false;

        return String.valueOf(builder);
    }
}
