package model;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Represent a request's record: its start time, end time, request type, response code
 */
@Data
@AllArgsConstructor
public class RequestRecord {
  private long startTime;
  private long endTime;
  private String requestType;
  private int responseCode;

  public long getLatency() {
    return endTime - startTime;
  }
}
