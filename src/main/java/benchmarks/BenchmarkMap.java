package benchmarks;

import org.apache.flink.api.common.functions.MapFunction;

/**
 * Class used for flink benchmark evaluations.
 * In order to use this performance evaluator a Flink sink must be created using ".addSink(new BenchmarkFlinkSink())"
 */
public class BenchmarkMap implements MapFunction<String, String> {

	@Override
	public String map(String s) throws Exception {
		SynchronizedCounter synchronizedCounter = new SynchronizedCounter();
		return synchronizedCounter.incrementCounter();
	}
}
