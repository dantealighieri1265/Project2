package benchmarks;

import org.apache.flink.api.common.functions.MapFunction;

/**
 * Classe usata per le valutazioni dei Benchmark
 */
public class BenchmarkMap implements MapFunction<String, String> {

	@Override
	public String map(String s) throws Exception {
		SynchronizedCounter1 synchronizedCounter1 = new SynchronizedCounter1();
		return synchronizedCounter1.incrementCounter();
	}
}
