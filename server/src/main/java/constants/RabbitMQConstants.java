package constants;

public class RabbitMQConstants {
  public static final String DEFAULT_HOST = "localhost"; // Default host if not provided
  public static final int DEFAULT_PORT = 5672; // Default port if not provided
  public static final String DEFAULT_USERNAME = "guest"; // Default username if not provided
  public static final String DEFAULT_PASSWORD = "guest"; // Default password if not provided
  public static final String QUEUE_NAME = "liftRideQueue"; // Specify the queue name
  public static final int MAX_POOL_SIZE = 5; // adj base on load
  public static final int CHANNELS_PER_CONNECTION = 10;
  public static final int NUM_CONSUMERS = 10;
  public static final int PREFETCH_COUNT = 250;
  public static final int MESSAGE_CHUNK_SIZE = 1000;
}
