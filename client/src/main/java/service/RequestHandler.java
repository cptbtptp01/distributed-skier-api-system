package service;

import io.swagger.client.ApiException;
import io.swagger.client.ApiResponse;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.Random;

public class RequestHandler {
  private static final int MAX_RETRIES = 5;
  private static final int INITIAL_BACKOFF_MS = 100;
  private static final int MAX_BACKOFF_MS = 3000;
  private static final Random random = new Random();
  private static final AtomicInteger activeConnections = new AtomicInteger(0);
  private static final int MAX_CONCURRENT_CONNECTIONS = 150; // Tune this based on server capacity

  public static ApiResponse<Void> executeWithRetry(RequestAction action)
      throws InterruptedException, ApiException {
    int retries = 0;
    int backoffTime = INITIAL_BACKOFF_MS;
    ApiResponse<Void> response = null;

    while (retries < MAX_RETRIES) {
      try {
        // Wait if too many active connections
        while (activeConnections.get() >= MAX_CONCURRENT_CONNECTIONS) {
          TimeUnit.MILLISECONDS.sleep(50);
        }

        activeConnections.incrementAndGet();
        try {
          response = action.execute();
          if (response.getStatusCode() >= 200 && response.getStatusCode() < 300) {
            return response;
          }
        } finally {
          activeConnections.decrementAndGet();
        }

      } catch (ApiException e) {
        if (isRetryable(e)) {
          retries++;
          if (retries == MAX_RETRIES) {
            throw e;
          }

          // Calculate exponential backoff with jitter
          backoffTime = Math.min(MAX_BACKOFF_MS, backoffTime * 2);
          int jitter = random.nextInt(50); // Add random jitter
          TimeUnit.MILLISECONDS.sleep(backoffTime + jitter);

          // Log retry attempt
          System.out.printf("Retry attempt %d after %dms for error: %s%n",
              retries, backoffTime, e.getMessage());
        } else {
          throw e; // Non-retryable error
        }
      }
    }

    return response;
  }

  private static boolean isRetryable(ApiException e) {
    // Consider certain status codes as retryable
    return e.getCode() == 0 || // Connection errors
        e.getCode() == 429 || // Too many requests
        e.getCode() >= 500; // Server errors
  }

  @FunctionalInterface
  public interface RequestAction {
    ApiResponse<Void> execute() throws ApiException;
  }
}
