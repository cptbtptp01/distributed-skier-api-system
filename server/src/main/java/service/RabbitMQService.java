package service;

import static constants.RabbitMQConstants.CHANNELS_PER_CONNECTION;
import static constants.RabbitMQConstants.MAX_POOL_SIZE;
import static constants.RabbitMQConstants.QUEUE_NAME;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * RabbitMQService provides a connection and channel management service for interacting with
 * RabbitMQ messaging system.
 * <p>
 * It maintains a pool of connections and channels to optimize resource usage and improve
 * performance when publishing messages to the RabbitMQ queue.
 */
public class RabbitMQService {
  private final ConnectionFactory factory;
  private final BlockingQueue<Connection> connectionPool; // Pool of connections
  private final ConcurrentHashMap<Connection, BlockingQueue<Channel>> channelPools; // Mapping of connections to channel pools

  /**
   * Constructs a RabbitMQService with a specified ConnectionFactory.
   *
   * @param factory the ConnectionFactory used to create RabbitMQ connections
   * @throws IOException      if an I/O error occurs during connection or channel creation
   * @throws TimeoutException if the connection attempt times out
   */
  public RabbitMQService(ConnectionFactory factory) throws IOException, TimeoutException {
    this.factory = factory;
    this.connectionPool = new LinkedBlockingQueue<>(MAX_POOL_SIZE);
    this.channelPools = new ConcurrentHashMap<>();
    initializeConnections();
  }

  /**
   * Prepopulate the connection and channel pools
   * @throws IOException
   * @throws TimeoutException
   */
  private void initializeConnections() throws IOException, TimeoutException {

    for (int i = 0; i < MAX_POOL_SIZE; i++) {
      Connection connection = factory.newConnection();
      connectionPool.offer(connection);

      BlockingQueue<Channel> channelPool = new LinkedBlockingQueue<>(CHANNELS_PER_CONNECTION);
      for (int j = 0; j < CHANNELS_PER_CONNECTION; j++) {
        Channel channel = connection.createChannel();
        channel.queueDeclare(QUEUE_NAME, true, false, false, null);
        channelPool.offer(channel);
      }
      // map connection to its channel pool
      channelPools.put(connection, channelPool);
    }
  }

  /**
   * Retrieves a connection from the connection pool.
   *
   * @return a Connection object
   * @throws InterruptedException if the current thread is interrupted while waiting
   */
  public Connection getConnection() throws InterruptedException {
    return connectionPool.take();
  }

  /**
   * Returns a connection back to the connection pool.
   *
   * @param connection the Connection to return to the pool
   */
  public void returnConnection(Connection connection) {
    connectionPool.offer(connection);
  }

  /**
   * Retrieves a channel from the specified connection's channel pool.
   *
   * @param connection the Connection from which to retrieve a channel
   * @return a Channel object
   * @throws InterruptedException if the current thread is interrupted while waiting
   */
  public Channel getChannel(Connection connection) throws InterruptedException {
    // Try 5 secs
    Channel channel = channelPools.get(connection).poll(5, TimeUnit.SECONDS);
    return channel;
  }

  /**
   * Returns a channel back to the specified connection's channel pool.
   *
   * @param connection the Connection to which the channel belongs
   * @param channel    the Channel to return to the pool
   */
  public void returnChannel(Connection connection, Channel channel) {
    channelPools.get(connection).offer(channel);
  }

  /**
   * Publishes a message to the RabbitMQ queue.
   *
   * @param channel the Channel used to publish the message
   * @param message the message to publish
   * @return true if the message was successfully published, false otherwise
   */
  public boolean publishMessage(Channel channel, String message) {
    try {
      // Publish message to the specified queue
      channel.basicPublish("", QUEUE_NAME, null, message.getBytes(StandardCharsets.UTF_8));
      return true;
    } catch (IOException e) {
      e.printStackTrace();
      return false;
    }
  }

  /**
   * Closes all channels and connections in the pools.
   */
  public void close() {
    channelPools.forEach((connection, channels) -> {
      channels.forEach(channel -> {
        try {
          channel.close();
        } catch (IOException | TimeoutException e) {
          e.printStackTrace();
        }
      });
      try {
        connection.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    });
  }
}