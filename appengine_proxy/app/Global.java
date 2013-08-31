import java.io.IOException;

import play.Application;
import play.GlobalSettings;
import yarn.YarnClient;


public class Global extends GlobalSettings {
  @Override
  public void onStart(Application app) {
    YarnClient client = new YarnClient();
    try {
      client.submitApplication("sh start", "/Users/hadoop/project/github/pivotal_hackday/appengine_proxy/dist/appengine_proxy-1.0.zip");
    } catch (IOException e) {
      e.printStackTrace();
      System.exit(-1);
    }
  }
}
