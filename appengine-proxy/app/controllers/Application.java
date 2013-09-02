package controllers;

import java.util.*;

import org.mortbay.log.Log;

import play.mvc.*;
import play.data.*;
import static play.data.Form.*;
import play.*;

import views.html.*;
import yarn.YarnHelper;

import models.*;

/**
 * Manage a database of computers
 */
public class Application extends Controller {
    public static List<App> apps = new ArrayList<App>();
  
    /**
     * This result directly redirect to application home.
     */
    public static Result GO_HOME = redirect(
        routes.Application.list()
    );
    
    /**
     * Handle default path requests, redirect to computers list
     */
    public static Result index() {
        return GO_HOME;
    }

    /**
     * Display the paginated list of computers.
     *
     * @param page Current page number (starts from 0)
     * @param sortBy Column to be sorted
     * @param order Sort order (either asc or desc)
     * @param filter Filter applied on computer names
     */
    public static Result list() {
        String[] names = new String[apps.size()];
        String[] urls = new String[apps.size()];
        String[] states = new String[apps.size()];
        String[] types = new String[apps.size()];
        for (int i = 0; i < apps.size(); i++) {
          names[i] = apps.get(i).name;
          urls[i] = apps.get(i).url;
          states[i] = apps.get(i).state.name();
          types[i] = apps.get(i).type;
        }
        
        return ok(
            list.render(names, urls, states, types)
        );
    }
    
    public static Result result() {
      return GO_HOME;    
    }
    
    /**
     * Display the 'new computer form'.
     */
    public static Result create() {
        Form<App> appForm = form(App.class);
        return ok(
            createForm.render(appForm)
        );
    }
    
    /**
     * Handle the 'new computer form' submission 
     */
    public static Result save() {
        Form<App> appForm = form(App.class).bindFromRequest();
        final App app = appForm.get();
        
        // call AM allocate a container and run application
        Thread newAppThread = new Thread(new Runnable() {
          @Override
          public void run() {
            try {
              YarnHelper.getInstance().runNewApp(app);
            } catch (Exception e) {
              e.printStackTrace();
            }
          }
          
        });
        newAppThread.start();
        
        apps.add(app);
        
        return GO_HOME;
    }
}
            
