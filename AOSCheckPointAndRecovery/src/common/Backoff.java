/**
 * CS 6378.002 Advanced Operating Systems
 * Fall 2013
 * Project 2 - Checkpoint and Recovery - Koo and Toueg
 *
 * @author Kiran Gavali
 */

package common;

import java.util.Random;

/**
 * Class implements the Ethernet-style exponential backoff
 * 
 * @author Kiran
 * 
 */
public class Backoff {
	private final int minDelay, maxDelay; // msec
	private int limit;
	private final Random random;

	/**
	 * Constructor
	 * 
	 * @param minDelay
	 *            minimum delay in msec
	 * @param maxDelay
	 *            maximum delay in msec
	 */
	public Backoff(int minDelay, int maxDelay) {
		this.minDelay = minDelay;
		this.maxDelay = maxDelay;
		limit = minDelay;
		random = new Random();
	}

	public void backoff() throws InterruptedException {
		int delay = random.nextInt(limit);
		limit = Math.min(maxDelay, 2 * limit);
		// System.out.println("Backing off for delay= " + delay);
		Thread.sleep(delay);
	}
	
	
	public void reset() {
		limit = minDelay;
	}

}
