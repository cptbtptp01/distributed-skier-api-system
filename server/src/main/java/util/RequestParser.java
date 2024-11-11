package util;

import com.google.gson.Gson;
import java.io.BufferedReader;
import java.io.IOException;
import javax.servlet.http.HttpServletRequest;
import model.LiftRide;
import model.LiftRideMessage;

public class RequestParser {

  private static final Gson gson = new Gson();

  public static String readRequestBody(HttpServletRequest request) throws IOException {
    StringBuilder buffer = new StringBuilder();
    BufferedReader reader = request.getReader();
    String line;
    while ((line = reader.readLine()) != null) {
      buffer.append(line);
    }
    return buffer.toString();
  }

  public static LiftRide parseLiftRide(String requestBody) {
    return gson.fromJson(requestBody, LiftRide.class);
  }

  public static String createMessageJson(LiftRideMessage message) {
    return gson.toJson(message);
  }
}
