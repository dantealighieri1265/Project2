package queries.query3;

import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import java.util.*;

public class Query3SortProcess extends ProcessAllWindowFunction<Query3Result,
        TreeMap<Double, List<Query3Result>>, TimeWindow> {

    @Override
    public void process(Context context, Iterable<Query3Result> iterable,
                        Collector<TreeMap<Double, List<Query3Result>>> collector) {

        TreeMap<Double, List<Query3Result>> ranking = new TreeMap<>(Collections.reverseOrder());

        for (Query3Result result: iterable){
            List<Query3Result> tripIds = ranking.computeIfAbsent(result.getDistance(), k -> new ArrayList<>());
            tripIds.add(result);
        }
        collector.collect(ranking);
    }
}
