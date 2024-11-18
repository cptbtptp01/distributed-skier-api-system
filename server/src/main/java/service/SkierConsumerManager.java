package service;

import static constants.RabbitMQConstants.DEFAULT_PASSWORD;
import static constants.RabbitMQConstants.DEFAULT_PORT;
import static constants.RabbitMQConstants.DEFAULT_USERNAME;
import static constants.RabbitMQConstants.HOST;
import static constants.RabbitMQConstants.MESSAGE_CHUNK_SIZE;
import static constants.RabbitMQConstants.NUM_CONSUMERS;
import static constants.RabbitMQConstants.PREFETCH_COUNT;
import static constants.RabbitMQConstants.QUEUE_NAME;

import com.google.common.util.concurrent.RateLimiter;
import com.google.gson.Gson;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import model.LiftRideMessage;

public class SkierConsumerManager {

  private final Connection connection;
  private final ExecutorService consumerExecutor;
  private final List<Channel> channels;
  private final AtomicInteger processedMessages;
  private final Gson gson;
  private final DynamoDBManager dynamoDBManager;

  // Throttling configuration
  private final RateLimiter rateLimiter;
  private static final int REQUESTS_PER_SECOND = 4000;
  private static final int INITIAL_QOS = 10;
  private static final int MAX_QOS = 50;
  private static final int MIN_QOS = 5;
  private final Map<Channel, Integer> channelQoS = new ConcurrentHashMap<>();


  public SkierConsumerManager(String host, int port, String username, String password)
      throws IOException, TimeoutException {
    // Initialize connection factory
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost(host);
    factory.setPort(port);
    factory.setUsername(username);
    factory.setPassword(password);

    // Create thread pool for consumers
    this.consumerExecutor = Executors.newFixedThreadPool(NUM_CONSUMERS);

    // Create single connection with consumer thread pool
    this.connection = factory.newConnection(consumerExecutor);

    this.channels = new CopyOnWriteArrayList<>();
    this.processedMessages = new AtomicInteger(0);
    this.gson = new Gson();

    this.dynamoDBManager = new DynamoDBManager();
    this.rateLimiter = RateLimiter.create(REQUESTS_PER_SECOND);

    // Initialize consumers
    initializeConsumers();

  }

  private void initializeConsumers() throws IOException {
    for (int i = 0; i < NUM_CONSUMERS; i++) {
      Channel channel = connection.createChannel();

      // Declare queue
      channel.queueDeclare(QUEUE_NAME, true, false, false, null);

      // Set QoS (prefetch count) for better load balancing
      channel.basicQos(INITIAL_QOS);
      channelQoS.put(channel, INITIAL_QOS);

      // Store channel for cleanup
      channels.add(channel);

      // Create consumer
      setupConsumer(channel);
    }
  }

  private void setupConsumer(Channel channel) throws IOException {
    // Create consumer with manual acknowledgment
    DeliverCallback deliverCallback = (consumerTag, delivery) -> {
      try {
        rateLimiter.acquire();

        String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
        boolean processed = processMessage(message);

        if (processed) {
          // Acknowledge message
          channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
          increaseQoS(channel);

          // Log progress
          int processedRequest = processedMessages.incrementAndGet();
          if (processedRequest % MESSAGE_CHUNK_SIZE == 0) {
            System.out.printf("Processed %d messages\n", processedRequest);
          }
        } else {
          // Negative acknowledgment and decrease QoS
          channel.basicNack(delivery.getEnvelope().getDeliveryTag(), false, true);
          decreaseQoS(channel);
        }

      } catch (Exception e) {
        // Reject message and requeue if processing fails
        channel.basicNack(delivery.getEnvelope().getDeliveryTag(), false, true);
        decreaseQoS(channel);
        e.printStackTrace();
      }
    };

    // Start consuming with manual acknowledgment
    channel.basicConsume(QUEUE_NAME, false, deliverCallback, consumerTag -> {
    });
  }

  private void increaseQoS(Channel channel) {
    try {
      int currentQoS = channelQoS.get(channel);
      if (currentQoS < MAX_QOS) {
        int newQoS = Math.min(currentQoS + 5, MAX_QOS);
        channel.basicQos(newQoS);
        channelQoS.put(channel, newQoS);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private void decreaseQoS(Channel channel) {
    try {
      int currentQoS = channelQoS.get(channel);
      if (currentQoS > MIN_QOS) {
        int newQoS = Math.max(currentQoS - 5, MIN_QOS);
        channel.basicQos(newQoS);
        channelQoS.put(channel, newQoS);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private boolean processMessage(String message) {
    try {
      LiftRideMessage liftRide = gson.fromJson(message, LiftRideMessage.class);
      dynamoDBManager.addToBatch(liftRide);
      return true;
    } catch (Exception e) {
      System.err.println("Error processing message: " + e.getMessage());
      return false;
    }
  }

  public void shutdown() {
    // Shutdown consumer executor
    consumerExecutor.shutdown();
    try {
      if (!consumerExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
        consumerExecutor.shutdownNow();
      }
    } catch (InterruptedException e) {
      consumerExecutor.shutdownNow();
      Thread.currentThread().interrupt();
    }

    // Close all channels
    for (Channel channel : channels) {
      try {
        if (channel.isOpen()) {
          channel.close();
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    // Close connection
    try {
      if (connection.isOpen()) {
        connection.close();
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static void main(String[] args) {
    try {
      SkierConsumerManager consumer = new SkierConsumerManager(
          HOST,
          DEFAULT_PORT,
          DEFAULT_USERNAME,
          DEFAULT_PASSWORD
      );
      // test table connection
      consumer.dynamoDBManager.testTableAccess();

      // Add shutdown hook
      Runtime.getRuntime().addShutdownHook(new Thread(consumer::shutdown));

      System.out.println("Consumer started...");

      // Keep main thread alive
      Thread.currentThread().join();

    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
