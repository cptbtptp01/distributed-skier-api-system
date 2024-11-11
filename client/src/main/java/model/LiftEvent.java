package model;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class LiftEvent {
  private int skierID;
  private int resortID;
  private int liftID;
  private String seasonID;
  private String dayID;
  private int time;
}
