package service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import model.LiftRideMessage;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutRequest;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

/**
 * Initialize dynamoDb client
 */
public class DynamoDBManager {

  private static final Region DEFAULT_REGION = Region.US_WEST_2;
  private static final int MAX_BATCH_SIZE = 25;
  private final DynamoDbClient dynamoDb;
  private final String tableName = "skier_table2";
  private final BlockingQueue<WriteRequest> writeRequestQueue;
  private final ScheduledExecutorService batchProcessor;

  public DynamoDBManager() {
    this.dynamoDb = createDynamoDBClient();
    this.writeRequestQueue = new LinkedBlockingQueue<>();
    this.batchProcessor = Executors.newSingleThreadScheduledExecutor();

    startBatchProcessor();
  }

  /**
   * Create a DynamoDB client
   *
   * @return
   */
  private static DynamoDbClient createDynamoDBClient() {
    // retrieve keys from aws credentials
    return DynamoDbClient.builder().region(DEFAULT_REGION).credentialsProvider(
        ProfileCredentialsProvider.create()).build();
  }

  /**
   * Creates a thread that runs every 100ms, process items in the queue, continue running until
   * shutdown
   */
  private void startBatchProcessor() {
    batchProcessor.scheduleAtFixedRate(
        this::processBatch,
        0,
        100,  // Process every 100ms
        TimeUnit.MILLISECONDS
    );
  }

  public void addToBatch(LiftRideMessage message) {
    Map<String, AttributeValue> item = createItem(message);
    PutRequest putRequest = PutRequest.builder().item(item).build();
    WriteRequest writeRequest = WriteRequest.builder().putRequest(putRequest).build();

    // Producer threads(consumer) add items
    writeRequestQueue.offer(writeRequest);
    // Process batch when reach to max batch size
    if (writeRequestQueue.size() >= MAX_BATCH_SIZE) {
      processBatch();
    }
  }

  /**
   * Process batch writes
   */
  private void processBatch() {
    List<WriteRequest> batch = new ArrayList<>();

    // Consumer thread(scheduler) removes items, blocks if capacity reached
    writeRequestQueue.drainTo(batch, MAX_BATCH_SIZE);

    if (batch.isEmpty()) {
      return;
    }

    try {
      Map<String, List<WriteRequest>> requestItems = new HashMap<>();
      requestItems.put(tableName, batch);

      BatchWriteItemRequest batchWriteItemRequest = BatchWriteItemRequest.builder()
          .requestItems(requestItems)
          .build();

      // Implement exponential backoff
      int retries = 0;
      int maxRetries = 3;
      do {
        if (retries > 0) {
          long delay = (long) Math.min(100 * Math.pow(2, retries), 1000);
          Thread.sleep(delay);
        }

        BatchWriteItemResponse response = dynamoDb.batchWriteItem(batchWriteItemRequest);
        Map<String, List<WriteRequest>> unprocessedItems = response.unprocessedItems();

        if (unprocessedItems.isEmpty()) {
          break;
        }

        // Update request with unprocessed items
        batchWriteItemRequest = BatchWriteItemRequest.builder()
            .requestItems(unprocessedItems)
            .build();

        retries++;
      } while (retries < maxRetries);

    } catch (Exception e) {
      System.err.println("Error in batch processing: " + e.getMessage());
      // Return failed items to the queue
      batch.forEach(writeRequestQueue::offer);
    }
  }

  public void shutdown() {
    batchProcessor.shutdown();
    try {
      if (!batchProcessor.awaitTermination(30, TimeUnit.SECONDS)) {
        batchProcessor.shutdownNow();
      }
      // Process any remaining items
      processBatch();
    } catch (InterruptedException e) {
      batchProcessor.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Save data to table
   *
   * @param message
   */
  public void saveItem(LiftRideMessage message) {
    try {
      Map<String, AttributeValue> item = createItem(message);
      PutItemRequest request = PutItemRequest.builder().tableName(tableName).item(item).build();
      dynamoDb.putItem(request);
    } catch (DynamoDbException e) {
      System.err.println("Error writing item to table: " + e.getMessage());
      throw e;
    }
  }

  private Map<String, AttributeValue> createItem(LiftRideMessage message) {
    Map<String, AttributeValue> item = new HashMap<>();
    String seasonDay = message.getSeasonId() + "_" + message.getDayId();

    item.put("skier_id", AttributeValue.builder().s(String.valueOf(message.getSkierId())).build());
    item.put("season_day", AttributeValue.builder().s(seasonDay).build());
    item.put("resort_id",
        AttributeValue.builder().n(String.valueOf(message.getResortId())).build());
    item.put("time",
        AttributeValue.builder().n(String.valueOf(message.getLiftRide().getTime())).build());
    item.put("lift_id",
        AttributeValue.builder().n(String.valueOf(message.getLiftRide().getLiftID())).build());

    return item;
  }

  /**
   * Verify table
   *
   * @return
   */
  public void testTableAccess() {
    try {
      DescribeTableRequest request = DescribeTableRequest.builder()
          .tableName(tableName)
          .build();

      DescribeTableResponse response = dynamoDb.describeTable(request);
      System.out.println("Table status: " + response.table().tableStatus());
      System.out.println("Item count: " + response.table().itemCount());
      System.out.println("Size in bytes: " + response.table().tableSizeBytes());
    } catch (ResourceNotFoundException e) {
      System.err.println(e.getMessage());
    } catch (DynamoDbException e) {
      System.err.println("Error accessing table: " + e.getMessage());
    }
  }
}
