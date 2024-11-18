package thread;

import static constants.RabbitMQConstants.MESSAGE_CHUNK_SIZE;
import static constants.RabbitMQConstants.PREFETCH_COUNT;
import static constants.RabbitMQConstants.QUEUE_NAME;

import com.google.gson.Gson;
import com.rabbitmq.client.CancelCallback;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DeliverCallback;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import model.LiftRideMessage;
import service.DynamoDBManager;

public class ConsumerThread implements Runnable {

  private final Connection connection;
  private final AtomicInteger processedMessages;
  private final Gson gson = new Gson();
  private final DynamoDBManager dynamoDBManager;
  private Channel channel;

  public ConsumerThread(Connection connection, AtomicInteger processedMessages,
      DynamoDBManager dynamoDBManager) {
    this.connection = connection;
    this.processedMessages = processedMessages;
    this.dynamoDBManager = dynamoDBManager;
  }

  @Override
  public void run() {
    try {
      channel = connection.createChannel();
      // booleans: durable, exclusive, autoDelete
      channel.queueDeclare(QUEUE_NAME, true, false, false, null);
      channel.basicQos(PREFETCH_COUNT);

      DeliverCallback deliverCallback = (consumerTag, delivery) -> {

        String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
        try {
          processMessage(message);
          channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
          processedMessages.incrementAndGet();

          int processed = processedMessages.incrementAndGet();
          if (processed % MESSAGE_CHUNK_SIZE == 0) {
            System.out.println("Processed" + processed + "messages");
          }
        } catch (Exception e) {
          System.err.println("Failed to process message: " + e.getMessage());
          // Reject message and requeue if processing fails
          channel.basicNack(delivery.getEnvelope().getDeliveryTag(), false, true);
        }
      };

      channel.basicConsume(QUEUE_NAME, false, deliverCallback, (consumerTag) -> {
      });
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private void processMessage(String message) {
    LiftRideMessage liftRideMessage = gson.fromJson(message, LiftRideMessage.class);
    dynamoDBManager.saveItem(liftRideMessage);
  }

  public void shutdown() {
    try {
      if (channel != null && channel.isOpen()) {
        channel.close();
      }
    } catch (IOException | TimeoutException e) {
      e.printStackTrace();
    }
  }
}
