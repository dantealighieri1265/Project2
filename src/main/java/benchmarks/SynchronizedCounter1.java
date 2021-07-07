package benchmarks;

/**
 * Static counter for benchmark evaluation
 */
public class SynchronizedCounter1 {

	// contatore delle tuple
	private static long counter = 0L;
	// tempo iniziale della finestra considerata
	private static long startTime;
	StringBuilder builder = new StringBuilder();


	/**
	 * Incrementa atomicamente gli attributi
	 */
	public synchronized String incrementCounter() {

		if (counter == 0L) {
			startTime = System.currentTimeMillis();
		}
		counter++;
		double currentTime = System.currentTimeMillis() - startTime;
		builder.append("Mean throughput: ").append(counter / currentTime).append("\n").append("Mean latency: ").append(currentTime / counter);
		builder.append("\n");
		return builder.toString();
	}
}
