package config;

import static constants.RabbitMQConstants.DEFAULT_HOST;
import static constants.RabbitMQConstants.DEFAULT_PASSWORD;
import static constants.RabbitMQConstants.DEFAULT_PORT;
import static constants.RabbitMQConstants.DEFAULT_USERNAME;

import com.rabbitmq.client.ConnectionFactory;

/**
 * class to set up config for Rabbit MQ
 */
public class RabbitMQConfig {


  public static ConnectionFactory createFactory() {
    ConnectionFactory factory = new ConnectionFactory();

    // Get RabbitMQ configurations from environment variables or use defaults
    String host =
        System.getenv("RABBITMQ_HOST") != null ? System.getenv("RABBITMQ_HOST") : DEFAULT_HOST;
    String port = System.getenv("RABBITMQ_PORT") != null ? System.getenv("RABBITMQ_PORT")
        : String.valueOf(DEFAULT_PORT);
    String username =
        System.getenv("RABBITMQ_USERNAME") != null ? System.getenv("RABBITMQ_USERNAME")
            : DEFAULT_USERNAME;
    String password =
        System.getenv("RABBITMQ_PASSWORD") != null ? System.getenv("RABBITMQ_PASSWORD")
            : DEFAULT_PASSWORD;

    factory.setHost(host);
    factory.setPort(Integer.parseInt(port)); // Convert port to int
    factory.setUsername(username);
    factory.setPassword(password);

    return factory;
  }
}