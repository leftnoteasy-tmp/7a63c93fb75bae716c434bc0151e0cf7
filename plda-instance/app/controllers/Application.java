package controllers;

import java.util.*;

import org.mortbay.log.Log;

import play.mvc.*;
import play.data.*;
import static play.data.Form.*;
import play.*;

import views.html.*;

import models.*;

/**
 * Manage a database of computers
 */
public class Application extends Controller {
    public static List<App> apps = new ArrayList<App>();
    
    /**
     * Handle default path requests, redirect to computers list
     */
    public static Result index() {
        return ok(
            index.render()
        );
    }
    
    public static Result train() {
      Form<TrainRequest> trainForm = form(TrainRequest.class);
      return ok(
          createtrain.render(trainForm)
      );
    }
    
    public static Result trainSave() {
      Form<TrainRequest> trainForm = form(TrainRequest.class).bindFromRequest();
      TrainRequest trainRequest = trainForm.get();
      
      // here we add code call training
      
      return index();
    }
    
    public static Result getTrainResult() {
      final String[] words = new String[] { "aaa", "bbB"};
      final int[] width1 = new int[] { 20, 30 };
      final int[] width2 = new int[] { 30, 40 };
      final int[] width3 = new int[] { 10, 10 };
      return ok(
          trainresult.render(words, width1, width2, width3)
      );
    }
    
    public static Result infer() {
      return null;
    }
    
    public static Result inferSave() {
      return null;
    }
    
    public static Result getInferResult() {
      return null;
    }
}
            
