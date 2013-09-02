import play.*;
import play.libs.*;
import yarn.YarnHelper;

import java.io.IOException;
import java.util.*;

import com.avaje.ebean.*;

import models.*;

public class Global extends GlobalSettings {
  @Override
  public void onStart(Application app) {
    if (false) {
      YarnHelper yarnHelper = YarnHelper.getInstance();
      try {
        yarnHelper.registerAM();
      } catch (IOException e) {
        e.printStackTrace();
        System.exit(-1);
      } catch (InterruptedException e) {
        e.printStackTrace();
        System.exit(-1);
      }
    }
  }
}