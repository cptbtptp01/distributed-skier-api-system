package constants;

public class RabbitMQConstants {

  public static final String DEFAULT_HOST = "localhost"; // Default host if not provided
  public static final int DEFAULT_PORT = 5672; // Default port if not provided
  public static final String DEFAULT_USERNAME = "guest"; // Default username if not provided
  public static final String DEFAULT_PASSWORD = "guest"; // Default password if not provided
  public static final String QUEUE_NAME = "liftRideQueue"; // Specify the queue name
  public static final int CHANNEL_POOL_SIZE = 50; // adj base on load
  public static final int CHANNEL_CHECKOUT_TIMEOUT = 5; // seconds
  public static final int NUM_CONSUMERS = 10;
  public static final int PREFETCH_COUNT = 10;
  public static final int MESSAGE_CHUNK_SIZE = 1000;

  public static final String HOST =
      System.getenv("RABBITMQ_HOST") != null ? System.getenv("RABBITMQ_HOST") : DEFAULT_HOST;
  public static final String USERNAME =
      System.getenv("RABBITMQ_USERNAME") != null ? System.getenv("RABBITMQ_USERNAME")
          : DEFAULT_USERNAME;
  public static final String PASSWORD =
      System.getenv("RABBITMQ_PASSWORD") != null ? System.getenv("RABBITMQ_PASSWORD")
          : DEFAULT_PASSWORD;
}
