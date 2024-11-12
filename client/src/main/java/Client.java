import static constants.ThreadConstants.PHASE1_NUM_OF_THREADS;
import static constants.ThreadConstants.PHASE1_REQUESTS_PER_THREAD;
import static constants.ThreadConstants.PHASE1_TOTAL_REQUESTS;
import static constants.ThreadConstants.PHASE2_NUM_OF_THREADS;
import static constants.ThreadConstants.PHASE2_REQUESTS_PER_THREAD;
import static constants.ThreadConstants.PHASE2_TOTAL_REQUESTS;
import static constants.ThreadConstants.TOTAL_REQUESTS;

import config.RequestThreadConfig;
import io.swagger.client.ApiClient;
import io.swagger.client.api.SkiersApi;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import model.LiftEvent;
import model.RequestRecord;
import service.ThreadMonitor;
import thread.EventThread;
import thread.RequestThread;

public class Client {

  private static final AtomicInteger successfulRequests = new AtomicInteger(0);
  private static final AtomicInteger failedRequests = new AtomicInteger(0);
  private static final BlockingQueue<LiftEvent> events = new LinkedBlockingQueue<>();
  private static final BlockingQueue<RequestRecord> requestLog = new LinkedBlockingQueue<>();

  public static void main(String[] args) {

    try {
      // Generate events
      // TODO consider multithread working on generating events?
      ExecutorService eventExecutor = Executors.newSingleThreadExecutor();
      EventThread eventThread = new EventThread(events, TOTAL_REQUESTS);
      eventExecutor.submit(eventThread);

      // Phase 1
      final long phase1Start = System.currentTimeMillis();
      doPhaseOne();
      final long phase1End = System.currentTimeMillis();
      final int phase1SuccessfulRequests = successfulRequests.get();
      final int phase1FailedRequests = failedRequests.get();


      // Phase 2
      final long phase2Start = System.currentTimeMillis();
      doPhaseTwo();
      final long phase2End = System.currentTimeMillis();

      // Get phase1 Stats
      printThreadConfig("Phase 1", PHASE1_NUM_OF_THREADS,
          PHASE1_TOTAL_REQUESTS);
      printResults(phase1SuccessfulRequests, phase1FailedRequests,
          phase1Start, phase1End);

      // Get phase2 Stats
      printThreadConfig("Phase 2", PHASE2_NUM_OF_THREADS, PHASE2_TOTAL_REQUESTS);
      printResults(successfulRequests.get() - phase1SuccessfulRequests,
          failedRequests.get() - phase1FailedRequests,
          phase2Start, phase2End);

      // Get total stats
      printThreadConfig("Phase 1 + Phase 2", PHASE2_NUM_OF_THREADS + PHASE1_NUM_OF_THREADS,
          TOTAL_REQUESTS);
      printResults(successfulRequests.get(), failedRequests.get(), phase1Start, phase2End);
      printLatencies();
      eventExecutor.shutdown();

    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Process requests base on thread count and request per thread
   *
   * @throws InterruptedException
   */
  private static void doPhaseOne()
      throws InterruptedException {
    ExecutorService requestExecutor = Executors.newFixedThreadPool(PHASE1_NUM_OF_THREADS);

    // Start request processing threads
    CountDownLatch threadLatch = new CountDownLatch(PHASE1_NUM_OF_THREADS);
    for (int i = 0; i < PHASE1_NUM_OF_THREADS; i++) {
      RequestThreadConfig threadConfig = generateThreadConfig(threadLatch,
          PHASE1_REQUESTS_PER_THREAD);
      requestExecutor.submit(new RequestThread(threadConfig));
    }

    // Wait for all request threads to complete
    threadLatch.await();

    // Shutdown executors
    requestExecutor.shutdown();
  }

  /**
   * Process requests and create thread dynamically base on remaining requests
   *
   * @throws InterruptedException
   */
  private static void doPhaseTwo() throws InterruptedException {
    //TODO threadpool excecutor vs ExecutorService
    ThreadPoolExecutor requestExecutor =
        (ThreadPoolExecutor) Executors.newFixedThreadPool(PHASE2_NUM_OF_THREADS);

    // Add monitoring thread
    Thread monitor = new Thread(new ThreadMonitor(requestExecutor,
        PHASE2_TOTAL_REQUESTS, successfulRequests, failedRequests));
    monitor.start();

    int remainingRequests = PHASE2_TOTAL_REQUESTS;
    CountDownLatch threadLatch = new CountDownLatch(PHASE2_NUM_OF_THREADS);

    try {
      while (remainingRequests > 0) {
        final int requestsToProcess = Math.min(remainingRequests, PHASE2_REQUESTS_PER_THREAD);
        RequestThreadConfig threadConfig = generateThreadConfig(threadLatch, requestsToProcess);
        requestExecutor.submit(new RequestThread(threadConfig));
        remainingRequests -= requestsToProcess;
      }

      // Add timeout to avoid infinite wait
      if (!threadLatch.await(5, TimeUnit.MINUTES)) {
        System.err.println("Timeout waiting for threads to complete!");
      }
    } finally {
      requestExecutor.shutdown();
      monitor.interrupt();

      // Try to wait for proper shutdown
      if (!requestExecutor.awaitTermination(60, TimeUnit.SECONDS)) {
        requestExecutor.shutdownNow();
      }
    }
  }

  private static RequestThreadConfig generateThreadConfig(CountDownLatch latch,
      int requestsPerThread) {
    // api client per thread
    final ApiClient apiClient = new ApiClient();
    apiClient.setBasePath(RequestThreadConfig.BASE_PATH);

    final SkiersApi skiersApi = new SkiersApi(apiClient);

    return new RequestThreadConfig(events, requestLog, latch, successfulRequests, failedRequests,
        skiersApi, requestsPerThread);
  }

  private static void printThreadConfig(final String phase, final int threadCount,
      final int requestCount) {
    System.out.println("-----------------------------------------------");
    System.out.println("Phase: " + phase);
    System.out.printf("Number of threads: %,d%n", threadCount);
    System.out.printf("Total Requests: %,d%n", requestCount);
  }

  private static void printResults(int success, int failed, long start, long end) {
    long wallTime = end - start;
    long throughput = (success + failed) > 0 ? 1000L * (success + failed) / wallTime : 0;
    System.out.printf("Number of successful requests sent: %,d%n", success);
    System.out.printf("Number of unsuccessful requests: %,d%n", failed);
    System.out.printf("The total run time (wall time): %,d ms%n", wallTime);
    System.out.printf("Throughput:  %,d req/sec\n", throughput);
  }

  private static void printLatencies() {
    List<Long> latencies = new ArrayList<>();
    for (RequestRecord record : requestLog) {
      latencies.add(record.getLatency());
    }

    Collections.sort(latencies);

    long min = latencies.get(0);
    long max = latencies.get(latencies.size() - 1);
    double mean = latencies.stream().mapToLong(Long::longValue).average().orElse(0.0);
    double median = latencies.get(latencies.size() / 2);
    double p99 = latencies.get((int) (latencies.size() * 0.99));
    System.out.println("-----------------------------------------------");
    System.out.println("Min response time (ms): " + min);
    System.out.println("Max response time (ms): " + max);
    System.out.println("Mean response time (ms): " + mean);
    System.out.println("Median response time (ms): " + median);
    System.out.println("99th percentile response time (ms): " + p99);
  }
}
