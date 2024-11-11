package constants;

/**
 * Constants for thread
 */
public class ThreadConstants {

  public static final int RETRY_TIMES = 5;
  public static final int TOTAL_REQUESTS = 200000;
  public static final int PHASE1_NUM_OF_THREADS = 32;
  public static final int PHASE2_NUM_OF_THREADS = 100;
  public static final int PHASE1_REQUESTS_PER_THREAD = 1000;
  public static final int INITIAL_RETRY_DELAY_MS = 1000;
  public static final long BACKOFF_MULTIPLIER = (long) 2.0;
  public static final int PHASE1_TOTAL_REQUESTS =
      PHASE1_NUM_OF_THREADS * PHASE1_REQUESTS_PER_THREAD;
  public static final int PHASE2_TOTAL_REQUESTS = TOTAL_REQUESTS - PHASE1_TOTAL_REQUESTS;
  public static final int PHASE2_REQUESTS_PER_THREAD = (int) Math.ceil(
      (float) PHASE2_TOTAL_REQUESTS / PHASE2_NUM_OF_THREADS);
}
