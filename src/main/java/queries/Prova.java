package queries;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class Prova {

	public static void main(String[] args) {
		
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<String, Integer>> dataStream = env
        		.readTextFile("../prj2_dataset.csv")
                .flatMap(new Splitter())
                .keyBy(value -> value.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(1)))
                .sum(1);

        dataStream.print();

        try {
			env.execute("Window WordCount");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }

    public static class Splitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        
        /**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		@Override
		public void flatMap(String sentence, Collector<Tuple2<String, Integer>> out) throws Exception {
            for (String word: sentence.split("\n")) {
                out.collect(new Tuple2<String, Integer>(word, 1));
            }
        }
    }

}
		/*final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
	    
	    // get input data
	    DataSet<String> text = env.fromElements(
	        "To be, or not to be,--that is the question:--",
	        "Whether 'tis nobler in the mind to suffer",
	        "The slings and arrows of outrageous fortune",
	        "Or to take arms against a sea of troubles,"
	        );
	    
	    DataSet<Tuple2<String, Integer>> counts = 
	        // split up the lines in pairs (2-tuples) containing: (word,1)
	        text.flatMap()
	        // group by the tuple field "0" and sum up tuple field "1"
	        .groupBy(0)
	        .aggregate(Aggregations.SUM, 1);

	    // emit result
	    try {
			counts.print();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	  }*/



