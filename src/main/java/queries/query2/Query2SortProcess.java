package queries.query2;

import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.time.ZoneId;
import java.util.*;

public class Query2SortProcess extends ProcessAllWindowFunction<Query2Result,
        List<TreeMap<String, Query2Result>>, TimeWindow> {

    @Override
    public void process(Context context, Iterable<Query2Result> iterable, Collector<List<TreeMap<String,
            Query2Result>>> collector) throws Exception {

        //todo sovrascrive la data di inzio finestra precedente: solo per effttuare un controllo
        Query2Result query2Result = iterable.iterator().next();
        query2Result.setStartDate(Instant.ofEpochMilli(context.window().getStart()).
                atZone(ZoneId.systemDefault()).toLocalDateTime());


        TreeMap<String, Query2Result> westAM = new TreeMap<>(Collections.reverseOrder());
        TreeMap<String, Query2Result> westPM = new TreeMap<>(Collections.reverseOrder());
        TreeMap<String, Query2Result> estAM = new TreeMap<>(Collections.reverseOrder());
        TreeMap<String, Query2Result> estPM = new TreeMap<>(Collections.reverseOrder());
        for (Query2Result result: iterable){
            //todo il formato non è parametrico. Se il numero di navi aumenta, non viene gestito
            westAM.put(String.format(Locale.ENGLISH, "%05d",result.getCountWestAM())+
                    ":"+result.getCellId(), result);
            westPM.put(String.format(Locale.ENGLISH, "%05d",result.getCountWestPM()) +
                    ":"+result.getCellId(), result);
            estAM.put(String.format(Locale.ENGLISH, "%05d",result.getCountEstAM())+
                    ":"+result.getCellId(), result);
            estPM.put(String.format(Locale.ENGLISH, "%05d",result.getCountEstPM())+
                    ":"+result.getCellId(), result);

        }
        List<TreeMap<String, Query2Result>> list = new ArrayList<>();
        list.add(westAM);
        list.add(westPM);
        list.add(estAM);
        list.add(estPM);

        collector.collect(list);
    }
}
