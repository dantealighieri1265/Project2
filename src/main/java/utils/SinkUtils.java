package utils;

import queries.query1.Query1Result;
import queries.query2.Query2Result;
import queries.query3.Query3Result;

import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class SinkUtils {

    public static final String QUERY1_OUTPUT_WEEKLY="results/Query1OutputWeekly.csv";
    public static final String QUERY1_OUTPUT_MONTHLY="results/Query1OutputMonthly.csv";
    public static final String QUERY2_OUTPUT_WEEKLY="results/Query2OutputWeekly.csv";
    public static final String QUERY2_OUTPUT_MONTHLY="results/Query2OutputMonthly.csv";
    public static final String QUERY3_OUTPUT_ONE_HOUR ="results/Query3OutputOneHour.csv";
    public static final String QUERY3_OUTPUT_TWO_HOUR ="results/Query3OutputTwoHour.csv";

    public final static String[] LIST_OUTPUT= {QUERY1_OUTPUT_WEEKLY, QUERY1_OUTPUT_MONTHLY, QUERY2_OUTPUT_WEEKLY,
            QUERY2_OUTPUT_MONTHLY, QUERY3_OUTPUT_ONE_HOUR, QUERY3_OUTPUT_TWO_HOUR};

    //todo mettere l'header ai csv

    //todo gestire gli zeri

    /**
     *
     * @param query1Result Risultati da stampare sul file
     * @return stringa contente i risultati formattati
     */
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

    /**
     *
     * @param list Risultati da stampare sul file
     * @return stringa contente i risultati formattati
     */
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

    /**
     *
     * @param treeMap Risultati da stampare sul file
     * @return stringa contente i risultati formattati
     */
    public static String createCSVQuery3(TreeMap<Double, List<Query3Result>> treeMap){
        StringBuilder builder = new StringBuilder();
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
                    if (checkVoidValue(index, result)){
                        break;
                    }
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

    private static boolean checkVoidValue(int index, Query2Result result) {
        switch (index){
            case 0:
                return result.getCountWestAM() == 0;
            case 1:
                return result.getCountWestPM() == 0;
            case 2:
                return result.getCountEstAM() == 0;
            case 3:
                return result.getCountEstPM() == 0;
        }
        return true;

    }

    public static String createCSVQuery2(queries.ktm.Query2Result query2Results) {
        //(treeAm e treePM)
        StringBuilder builder = new StringBuilder();
        builder.append(query2Results.getStartDate());

        final int[] count = {0};
        if (query2Results.getSea().equals("WEST")){
            builder.append(",");
            builder.append("West Mediterranean");
            builder.append(",");
            builder.append("AM");
            builder.append(",");
            final AtomicBoolean[] atLest = {new AtomicBoolean(false)};
            query2Results.getTreeMapAM().forEach((key, value) -> {

                for (String cell: value){
                    if (count[0] == 3)//se ho più di 3 elementi termino la classifica
                        break;
                    count[0]++;
                    atLest[0].set(true);
                    builder.append(cell);
                    builder.append("-");
                }


            });
            if (atLest[0].get())
                builder.deleteCharAt(builder.length()-1);



            builder.append(",");
            builder.append("PM");
            builder.append(",");
            count[0] = 0;
            atLest[0].set(false);
            query2Results.getTreeMapPM().forEach((key, value) -> {
                for (String cell: value){
                    if (count[0] == 3)//se ho più di 3 elementi termino la classifica
                        break;
                    count[0]++;
                    atLest[0].set(true);
                    builder.append(cell);
                    builder.append("-");
                }
                if (atLest[0].get())
                    builder.deleteCharAt(builder.length()-1);

            });


        }else {
            builder.append(",");
            builder.append("Est Mediterranean");
            builder.append(",");
            builder.append("AM");
            builder.append(",");
            final AtomicBoolean[] atLest = {new AtomicBoolean(false)};
            query2Results.getTreeMapAM().forEach((key, value) -> {

                for (String cell: value){
                    if (count[0] == 3)//se ho più di 3 elementi termino la classifica
                        break;
                    count[0]++;
                    atLest[0].set(true);
                    builder.append(cell);
                    builder.append("-");
                }

            });
            if (atLest[0].get())
                builder.deleteCharAt(builder.length()-1);


            builder.append(",");
            builder.append("PM");
            builder.append(",");
            count[0] = 0;
            atLest[0].set(false);

            query2Results.getTreeMapPM().forEach((key, value) -> {
                for (String cell: value){
                    if (count[0] == 3)//se ho più di 3 elementi termino la classifica
                        break;
                    count[0]++;
                    atLest[0].set(true);
                    builder.append(cell);
                    builder.append("-");
                }
                if (atLest[0].get())
                    builder.deleteCharAt(builder.length()-1);
            });

        }

        return builder.toString();
    }
}
