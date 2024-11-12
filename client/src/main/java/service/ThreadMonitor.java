package service;

import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ThreadMonitor implements Runnable {
  private final ThreadPoolExecutor executor;
  private final int expectedRequests;
  private final AtomicInteger successfulRequests;
  private final AtomicInteger failedRequests;

  public ThreadMonitor(ThreadPoolExecutor executor, int expectedRequests,
      AtomicInteger successfulRequests, AtomicInteger failedRequests) {
    this.executor = executor;
    this.expectedRequests = expectedRequests;
    this.successfulRequests = successfulRequests;
    this.failedRequests = failedRequests;
  }

  @Override
  public void run() {
    try {
      while (!executor.isTerminated()) {
        System.out.println(
            String.format("\nThread Pool Statistics:" +
                    "\nActive Threads: %d" +
                    "\nSuccessful Requests: %d" +
                    "\nFailed Requests: %d" +
                    "\nTotal Processed: %d/%d",
                executor.getActiveCount(),
                successfulRequests.get(),
                failedRequests.get(),
                successfulRequests.get() + failedRequests.get(),
                expectedRequests
            )
        );
        TimeUnit.SECONDS.sleep(5);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
