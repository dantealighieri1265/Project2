package benchmarks;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/**
 * Class used for flink benchmark evaluations.
 * In order to use this performance evaluator a Flink sink must be created using ".addSink(new BenchmarkFlinkSink())"
 */
public class BenchmarkFlinkSink implements SinkFunction<String> {

	@Override
	public void invoke(String value, Context context) {
		// stats counter
		benchmarks.SynchronizedCounter.incrementCounter();
	}
}
