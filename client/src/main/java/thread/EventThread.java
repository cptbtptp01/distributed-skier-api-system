package thread;

import static constants.LiftConstants.DAY_ID;
import static constants.LiftConstants.MAX_LIFT_ID;
import static constants.LiftConstants.MAX_RESORT_ID;
import static constants.LiftConstants.MAX_SKIER_ID;
import static constants.LiftConstants.MAX_TIME;
import static constants.LiftConstants.MIN_LIFT_ID;
import static constants.LiftConstants.MIN_SKIER_ID;
import static constants.LiftConstants.MIN_TIME;
import static constants.LiftConstants.SEASON_ID;

import java.util.Random;
import java.util.concurrent.BlockingQueue;
import model.LiftEvent;

/**
 * Responsible for generating lift events
 */
public class EventThread implements Runnable {

  private final BlockingQueue<LiftEvent> liftEventQueue;
  private final int totalEvents;
  private final Random random;

  public EventThread(BlockingQueue<LiftEvent> liftEventQueue, int totalEvents) {
    this.liftEventQueue = liftEventQueue;
    this.totalEvents = totalEvents;
    this.random = new Random();
  }

  @Override
  public void run() {
    // Generate events, add to queue per event count
    for (int i = 0; i < totalEvents; i++) {

      LiftEvent event = generateLiftEvent();

      try {
        this.liftEventQueue.put(event);
      } catch (Exception e) {
        System.err.println("Error occur when adding event to queue: " + e);
      }
    }
  }

  private LiftEvent generateLiftEvent() {
    int skierID = random.nextInt(MAX_SKIER_ID) + MIN_SKIER_ID;
    int resortID = random.nextInt(MAX_RESORT_ID) + MIN_SKIER_ID;
    int liftID = random.nextInt(MAX_LIFT_ID) + MIN_LIFT_ID;
    String seasonID = SEASON_ID;
    String dayID = DAY_ID;
    int time = random.nextInt(MAX_TIME) + MIN_TIME;

    LiftEvent event = new LiftEvent(skierID, resortID, liftID, seasonID, dayID, time);

    return event;
  }
}
