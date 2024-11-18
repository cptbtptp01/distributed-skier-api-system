package thread;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import model.RequestRecord;

/**
 * Responsible for handling request stat
 */
public class RecordThread implements Runnable {
  private BlockingQueue<RequestRecord> requestLog;

  public RecordThread(BlockingQueue<RequestRecord> requestLog) {
    this.requestLog = requestLog;
  }

  @Override
  public void run() {
    // write csv from request log, every 10secs?

  }

  private void writeToCSV(String filePath) {
    try (FileWriter writer = new FileWriter(filePath)) {
      writer.append("StartTime,RequestType,Latency,ResponseCode\n");
      for (RequestRecord record : requestLog) {
        writer.append(record.getStartTime() + "," + record.getRequestType() + "," + record.getLatency() + "," + record.getResponseCode() + "\n");
      }
      writer.flush();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private void printLatencies() {
    List<Long> latencies = new ArrayList<>();
    for (RequestRecord record : requestLog) {
      latencies.add(record.getLatency());
    }

    Collections.sort(latencies);

    long min = latencies.get(0);
    long max = latencies.get(latencies.size() - 1);
    double mean = latencies.stream().mapToLong(Long::longValue).average().orElse(0.0);
    double median = latencies.get(latencies.size() / 2);
    double p99 = latencies.get((int) (latencies.size() * 0.99));
    System.out.println("-----------------------------------------------");
    System.out.println("Min response time (ms): " + min);
    System.out.println("Max response time (ms): " + max);
    System.out.println("Mean response time (ms): " + mean);
    System.out.println("Median response time (ms): " + median);
    System.out.println("99th percentile response time (ms): " + p99);
  }
}
