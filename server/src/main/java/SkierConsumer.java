import static constants.RabbitMQConstants.MESSAGE_CHUNK_SIZE;
import static constants.RabbitMQConstants.NUM_CONSUMERS;
import static constants.RabbitMQConstants.PREFETCH_COUNT;
import static constants.RabbitMQConstants.QUEUE_NAME;

import com.google.gson.Gson;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import config.RabbitMQConfig;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import model.LiftRideMessage;

/**
 * The SkierConsumer class is designed to consume messages from a RabbitMQ queue (liftRideQueue)
 * that contains information about skier lift rides. The consumer processes messages concurrently
 * using a thread pool to enhance performance and handle multiple messages simultaneously.
 */
public class SkierConsumer {

  private static final ConcurrentHashMap<Integer, LiftRideMessage> skierRides = new ConcurrentHashMap<>();
  private final Gson gson = new Gson();
  private final ConnectionFactory factory;
  private final List<Channel> consumerChannels;
  private final ExecutorService threadPool;
  private final AtomicBoolean running = new AtomicBoolean(true);
  private AtomicInteger liftRideCount = new AtomicInteger(0);

  /**
   * Constructor initializes the consumer with RabbitMQ configuration
   *
   * @param factory
   * @throws IOException
   * @throws TimeoutException
   */
  public SkierConsumer(ConnectionFactory factory) throws IOException, TimeoutException {
    this.factory = factory;
    this.consumerChannels = new ArrayList<>();
    this.threadPool = Executors.newFixedThreadPool(NUM_CONSUMERS);

    initializeConsumers();

  }

  /**
   * Create multiple consumers on different channels for a connection
   * @throws IOException
   * @throws TimeoutException
   */
  private void initializeConsumers() throws IOException, TimeoutException {
    Connection connection = factory.newConnection();

    for (int i = 0; i < NUM_CONSUMERS; i++) {
      Channel channel = connection.createChannel();
      channel.queueDeclare(QUEUE_NAME, true, false, false, null);
      channel.basicQos(PREFETCH_COUNT); // Set prefetch count
      consumerChannels.add(channel);
    }

  }

  /**
   * Starts listening for messages from the designated RabbitMQ queue. This method will set up
   * consumers for each channel and begin processing messages as they arrive.
   *
   * @throws IOException If an I/O error occurs while starting the consumers.
   */
  public void startListening() throws IOException {
    System.out.println(" [*] Waiting for messages from queue: " + QUEUE_NAME);

    for (Channel channel : consumerChannels) {
      // Setup consumer for each channel
      channel.basicConsume(QUEUE_NAME, true, new DefaultConsumer(channel) {
        @Override
        public void handleDelivery(String consumerTag, Envelope envelope,
            AMQP.BasicProperties properties, byte[] body) {
          String message = new String(body, StandardCharsets.UTF_8);
          threadPool.submit(
              () -> processMessage(message)); // Process the message in a separate thread
        }
      });
    }
  }

  /**
   * Processes a received message by parsing it into a LiftRideMessage object and updating the skier
   * rides record.
   *
   * @param message The message received from the queue in JSON format.
   */
  private void processMessage(String message) {
    try {
      LiftRideMessage liftRideMessage = gson.fromJson(message, LiftRideMessage.class);
      skierRides.put(liftRideMessage.getSkierId(), liftRideMessage); // Store in the concurrent map

      liftRideCount.incrementAndGet();

      if (liftRideCount.get() % MESSAGE_CHUNK_SIZE == 0) {
        System.out.println(" [Processed " + skierRides.size() + " skiers, " + liftRideCount.get() + " lift rides]");
      }

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * Closes the consumer, shutting down all channels and the connection gracefully. This method also
   * shuts down the thread pool and waits for any ongoing tasks to complete.
   */
  public void close() {
    running.set(false);

    // Shutdown thread pool gracefully
    threadPool.shutdown();
    try {
      // Wait for tasks to complete with a timeout
      if (!threadPool.awaitTermination(30, TimeUnit.SECONDS)) {
        threadPool.shutdownNow();
      }
    } catch (InterruptedException e) {
      threadPool.shutdownNow();
      Thread.currentThread().interrupt();
    }

    // Close all channels
    for (Channel channel : consumerChannels) {
      try {
        if (channel != null && channel.isOpen()) {
          channel.close();
        }
      } catch (IOException | TimeoutException e) {
        e.printStackTrace();
      }
    }

    // Close the connection (it's shared by all channels)
    try {
      Connection connection = null;
      if (!consumerChannels.isEmpty()) {
        connection = consumerChannels.get(0).getConnection();
      }
      if (connection != null && connection.isOpen()) {
        connection.close();
      }
      System.out.println(" [x] All channels and connection closed");
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * Main method to start the SkierConsumer application. It initializes the consumer and starts
   * listening for messages from the RabbitMQ queue.
   *
   * @throws IOException      If an I/O error occurs during initialization.
   * @throws TimeoutException If the connection to RabbitMQ times out.
   */
  public static void main(String[] args) throws IOException, TimeoutException {
    ConnectionFactory factory = RabbitMQConfig.createFactory();

    SkierConsumer consumer = new SkierConsumer(factory);

    Runtime.getRuntime().addShutdownHook(new Thread(consumer::close));

    System.out.println("Consumer started");
    consumer.startListening();
  }
}