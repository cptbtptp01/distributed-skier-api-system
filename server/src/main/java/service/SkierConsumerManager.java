package service;

import static constants.RabbitMQConstants.DEFAULT_PASSWORD;
import static constants.RabbitMQConstants.DEFAULT_PORT;
import static constants.RabbitMQConstants.DEFAULT_USERNAME;
import static constants.RabbitMQConstants.HOST;
import static constants.RabbitMQConstants.MESSAGE_CHUNK_SIZE;
import static constants.RabbitMQConstants.NUM_CONSUMERS;
import static constants.RabbitMQConstants.PREFETCH_COUNT;
import static constants.RabbitMQConstants.QUEUE_NAME;

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
  private final Map<Integer, List<LiftRideMessage>> skierRecords;
  private final AtomicInteger processedMessages;
  private final Gson gson;

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
    this.skierRecords = new ConcurrentHashMap<>();
    this.processedMessages = new AtomicInteger(0);
    this.gson = new Gson();

    // Initialize consumers
    initializeConsumers();
  }

  private void initializeConsumers() throws IOException {
    for (int i = 0; i < NUM_CONSUMERS; i++) {
      Channel channel = connection.createChannel();

      // Declare queue
      channel.queueDeclare(QUEUE_NAME, true, false, false, null);

      // Set QoS (prefetch count) for better load balancing
      channel.basicQos(PREFETCH_COUNT);

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
        String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
        processMessage(message);

        // Acknowledge message
        channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);

        // Log progress
        int processed = processedMessages.incrementAndGet();
        if (processed % MESSAGE_CHUNK_SIZE == 0) {
          System.out.printf("Processed %d messages, tracking %d skiers%n",
              processed, skierRecords.size());
        }

      } catch (Exception e) {
        // Reject message and requeue if processing fails
        channel.basicNack(delivery.getEnvelope().getDeliveryTag(), false, true);
        e.printStackTrace();
      }
    };

    // Start consuming with manual acknowledgment
    channel.basicConsume(QUEUE_NAME, false, deliverCallback, consumerTag -> {
    });
  }

  private void processMessage(String message) {
    LiftRideMessage liftRide = gson.fromJson(message, LiftRideMessage.class);

    skierRecords.computeIfAbsent(liftRide.getSkierId(),
        k -> new CopyOnWriteArrayList<>()).add(liftRide);
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

      // Add shutdown hook
      Runtime.getRuntime().addShutdownHook(new Thread(consumer::shutdown));

      System.out.println("Consumer started.");

      // Keep main thread alive
      Thread.currentThread().join();

    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
