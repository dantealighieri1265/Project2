package benchmarks;

/**
 * Static counter for benchmark evaluation
 */
public class SynchronizedCounter {

	// tuples counter
	private static long counter = 0L;
	// first output time
	private static long startTime;
	StringBuilder builder = new StringBuilder();


	/**
	 * Called when a new observation is seen, updates statistics
	 */
	public synchronized String incrementCounter() {

		if (counter == 0L) {
			startTime = System.currentTimeMillis();
			System.out.println("Initialized!");
		}
		counter++;
		double currentTime = System.currentTimeMillis() - startTime;

		// prints mean throughput and latency so far evaluated
		/*System.out.println("Mean throughput: " + (counter/currentTime) + "\n" + "Mean latency: " +
				(currentTime/counter));*/

		builder.append("Mean throughput: ").append(counter / currentTime).append("\n").append("Mean latency: ").append(currentTime / counter);
		builder.append("\n");
		return builder.toString();
	}
}
