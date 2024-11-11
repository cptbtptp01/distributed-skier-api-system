package model;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class LiftRideMessage {

  private final LiftRide liftRide;
  private final int resortId;
  private final String seasonId;
  private final String dayId;
  private final int skierId;
}
