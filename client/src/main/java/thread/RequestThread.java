package thread;

import static constants.HttpConstants.HTTP_CREATED;
import static constants.HttpConstants.HTTP_OK;
import static constants.HttpConstants.POST;
import static constants.ThreadConstants.BACKOFF_MULTIPLIER;
import static constants.ThreadConstants.INITIAL_RETRY_DELAY_MS;
import static constants.ThreadConstants.RETRY_TIMES;

import config.RequestThreadConfig;
import io.swagger.client.ApiException;
import io.swagger.client.ApiResponse;
import io.swagger.client.model.LiftRide;
import java.util.concurrent.TimeUnit;
import model.LiftEvent;
import model.RequestRecord;

/**
 * Responsible for sending request to server
 */
public class RequestThread implements Runnable {

  private final RequestThreadConfig config;

  public RequestThread(final RequestThreadConfig config) {
    this.config = config;
  }

  @Override
  public void run() {
    try {
      // process all requests
      for (int i = 0; i < config.getNumOfRequests(); i++) {
        // process a request
        processRequestWithRetry();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      System.err.println("Thread interrupted: " + Thread.currentThread().getId());
    } finally {
      config.getCountDownLatch().countDown();
    }
  }

  /**
   * Process a single request with retry logic
   */
  private void processRequestWithRetry() throws InterruptedException {
    long reqStartTime = System.currentTimeMillis();
    int responseCode = 0;

    for (int attempt = 0; attempt < RETRY_TIMES; attempt++) {
      try {

        ApiResponse<Void> response = processRequest();
        responseCode = response.getStatusCode();

        if (responseCode == HTTP_CREATED || responseCode == HTTP_OK) {
          recordSuccess(reqStartTime, responseCode);
          return;
        }
      } catch (ApiException e) {
        responseCode = e.getCode();
        System.err.println("Attempt " + (attempt + 1) + " failed: " + e.getMessage());
      }
    }

    recordFailure(reqStartTime, responseCode);
  }

  /**
   * Record a successful request
   */
  private void recordSuccess(long startTime, int responseCode) throws InterruptedException {
    long endTime = System.currentTimeMillis();
    config.getSuccessfulRequests().incrementAndGet();
    config.getRecordsLog().put(new RequestRecord(startTime, endTime, POST, responseCode));
  }

  /**
   * Record a failed request
   */
  private void recordFailure(long startTime, int responseCode) throws InterruptedException {
    long endTime = System.currentTimeMillis();
    config.getFailedRequests().incrementAndGet();
    config.getRecordsLog().put(new RequestRecord(startTime, endTime, POST, responseCode));
  }

  /**
   * Send request to server, returns response
   *
   * @return Response
   * @throws InterruptedException if thread is interrupted
   * @throws ApiException         if API call fails
   */
  private ApiResponse<Void> processRequest()
      throws InterruptedException, ApiException {
    LiftEvent event = config.getEventQueue().take();
    LiftRide liftRide = new LiftRide().liftID(event.getLiftID()).time(event.getTime());

    ApiResponse<Void> res = config.getSkiersApi().writeNewLiftRideWithHttpInfo(liftRide,
        event.getResortID(), event.getSeasonID(), event.getDayID(), event.getSkierID());

    return res;
  }
}
