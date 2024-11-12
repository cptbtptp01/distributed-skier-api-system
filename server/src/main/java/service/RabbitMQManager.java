package service;

import static constants.RabbitMQConstants.CHANNEL_CHECKOUT_TIMEOUT;
import static constants.RabbitMQConstants.CHANNEL_POOL_SIZE;
import static constants.RabbitMQConstants.QUEUE_NAME;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class RabbitMQManager {


  private final Connection connection;
  private final BlockingQueue<Channel> channelPool;

  public RabbitMQManager(String host, int port, String username, String password)
      throws IOException, TimeoutException {
    // Create and configure connection factory
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost(host);
    factory.setPort(port);
    factory.setUsername(username);
    factory.setPassword(password);

    // Create single connection
    this.connection = factory.newConnection();
    this.channelPool = new LinkedBlockingQueue<>(CHANNEL_POOL_SIZE);

    // Initialize channel pool
    initializeChannelPool();
  }

  private void initializeChannelPool() throws IOException {
    for (int i = 0; i < CHANNEL_POOL_SIZE; i++) {
      Channel channel = connection.createChannel();
      // Declare queue - setting durable to true for message persistence
      channel.queueDeclare(QUEUE_NAME, true, false, false, null);
      channelPool.offer(channel);
    }
  }

  public Channel getChannel() throws InterruptedException {
    Channel channel = channelPool.poll(CHANNEL_CHECKOUT_TIMEOUT, TimeUnit.SECONDS);
    if (channel == null) {
      throw new InterruptedException("Timeout waiting for available channel");
    }
    return channel;
  }

  public void returnChannel(Channel channel) {
    if (channel != null && channel.isOpen()) {
      channelPool.offer(channel);
    }
  }

  public boolean publishMessage(String message) {
    Channel channel = null;
    try {
      channel = getChannel();
      channel.basicPublish("", QUEUE_NAME, null,
          message.getBytes(StandardCharsets.UTF_8));
      return true;
    } catch (Exception e) {
      e.printStackTrace();
      return false;
    } finally {
      returnChannel(channel);
    }
  }

  public void close() {
    // Close all channels
    channelPool.forEach(channel -> {
      try {
        if (channel.isOpen()) {
          channel.close();
        }
      } catch (IOException | TimeoutException e) {
        e.printStackTrace();
      }
    });

    // Close connection
    try {
      if (connection.isOpen()) {
        connection.close();
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
