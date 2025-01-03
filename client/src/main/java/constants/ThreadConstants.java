package constants;

/**
 * Constants for thread
 */
public class ThreadConstants {

  public static final int RETRY_TIMES = 5;
  public static final int TOTAL_REQUESTS = 200000;
  public static final int PHASE1_NUM_OF_THREADS = 32;
  public static final int PHASE2_NUM_OF_THREADS = 256;
  public static final int PHASE1_REQUESTS_PER_THREAD = 1000;
  public static final String CSV_FILE_PATH = "src/main/out";
  public static final int PHASE1_TOTAL_REQUESTS =
      PHASE1_NUM_OF_THREADS * PHASE1_REQUESTS_PER_THREAD;
  public static final int PHASE2_TOTAL_REQUESTS = TOTAL_REQUESTS - PHASE1_TOTAL_REQUESTS;
  public static final int PHASE2_REQUESTS_PER_THREAD = (int) Math.ceil(
      (float) PHASE2_TOTAL_REQUESTS / PHASE2_NUM_OF_THREADS);
  // For exponential backoff
  public static final long INITIAL_BACKOFF_MS = 100;
  public static final long MAX_BACKOFF_MS = 10000;
  public static final double BACKOFF_MULTIPLIER = 2.0;
  public static final double JITTER_FACTOR = 0.1; // randomize for all clients
}
