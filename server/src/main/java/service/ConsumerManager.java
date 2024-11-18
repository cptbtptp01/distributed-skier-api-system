package service;

import static constants.RabbitMQConstants.*;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import thread.ConsumerThread;

public class ConsumerManager {

  private final AtomicInteger processedMessages = new AtomicInteger(0);
  private final DynamoDBManager dynamoDBManager = new DynamoDBManager();
  private final ExecutorService rabbitExecutor;
  private final ExecutorService consumerExecutor;
  private final Connection connection;
  private final List<ConsumerThread> consumers;

  public ConsumerManager() throws IOException, TimeoutException {
    // Initialize connection factory
    ConnectionFactory connectionFactory = new ConnectionFactory();
    connectionFactory.setHost(HOST);
    connectionFactory.setPort(DEFAULT_PORT);
    connectionFactory.setUsername(DEFAULT_USERNAME);
    connectionFactory.setPassword(DEFAULT_PASSWORD);

    this.rabbitExecutor = Executors.newFixedThreadPool(NUM_CONSUMERS);
    this.connection = connectionFactory.newConnection(rabbitExecutor);
    this.consumerExecutor = Executors.newFixedThreadPool(NUM_CONSUMERS);
    this.consumers = new ArrayList<>();
  }

  public void start() {
    // Test DB connection
    dynamoDBManager.testTableAccess();

    // Start consumers
    for (int i = 0; i < NUM_CONSUMERS; i++) {
      ConsumerThread consumer = new ConsumerThread(connection, processedMessages, dynamoDBManager);
      consumers.add(consumer);
      consumerExecutor.execute(consumer);
    }
  }

  public void shutdown() {
    // Shutdown consumers
    for (ConsumerThread consumer : consumers) {
      consumer.shutdown();
    }

    // Shutdown executors
    consumerExecutor.shutdown();
    rabbitExecutor.shutdown();
    try {
      if (!consumerExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
        consumerExecutor.shutdownNow();
      }
      if (!rabbitExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
        rabbitExecutor.shutdownNow();
      }
    } catch (InterruptedException e) {
      consumerExecutor.shutdownNow();
      rabbitExecutor.shutdownNow();
      Thread.currentThread().interrupt();
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
      ConsumerManager manager = new ConsumerManager();

      // Add shutdown hook
      Runtime.getRuntime().addShutdownHook(new Thread(manager::shutdown));

      // Start consumers
      manager.start();

      System.out.println("Consumer started.");

      // Keep main thread alive
      Thread.currentThread().join();

    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}