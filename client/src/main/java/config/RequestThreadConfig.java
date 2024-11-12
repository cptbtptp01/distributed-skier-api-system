package config;


import static constants.HttpConstants.DEFAULT_HOST;

import io.swagger.client.api.SkiersApi;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.AllArgsConstructor;
import lombok.Data;
import model.LiftEvent;
import model.RequestRecord;

/**
 * Configs for a request thread
 */
@AllArgsConstructor
@Data
public class RequestThreadConfig {

  public static final String HOST =
      System.getenv("HOST") != null ? System.getenv("HOST") : DEFAULT_HOST;
  public static final String BASE_PATH =
      "http://" + HOST + (HOST.equals(DEFAULT_HOST) ? "/server_war_exploded"
          : "/server_war");

  private final BlockingQueue<LiftEvent> eventQueue;
  private final BlockingQueue<RequestRecord> recordsLog;
  private final CountDownLatch countDownLatch;
  private final AtomicInteger successfulRequests;
  private final AtomicInteger failedRequests;
  private final SkiersApi skiersApi;
  private final Integer numOfRequests;
}
