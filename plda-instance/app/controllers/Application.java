package controllers;

import static play.data.Form.form;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import math.WidthHelper;
import math.WordScore;
import models.App;
import models.InferRequest;
import models.TrainRequest;
import play.data.Form;
import play.mvc.Controller;
import play.mvc.Result;
import topic.InferResult;
import topic.Master;
import views.html.createinfer;
import views.html.createtrain;
import views.html.index;
import views.html.inferresult;
import views.html.trainresult;

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
      Master master = Master.getInstance();
      try {
        master.train("/Users/hadoop/Test/data/plda_data/training_doc_set", "/Users/hadoop/project/plda/mpi_lda", 3);
      } catch (IOException e) {
        e.printStackTrace();
        System.exit(-1);
      } catch (InterruptedException e) {
        e.printStackTrace();
        System.exit(-1);
      }
      
      return index();
    }
    
    public static Result getTrainResult() {
      if (Master.getInstance().getTrainFinished()) {
        Master master = Master.getInstance();
        Map<String, ArrayList<Integer>> result = master.getTrainingResult();
        List<WordScore> scores = WidthHelper.getWordScores(result, 100);
        final String[] words = WidthHelper.getWords(scores);
        final int[] width1 = WidthHelper.getWidths(scores, 0);
        final int[] width2 = WidthHelper.getWidths(scores, 1);
        final int[] width3 = WidthHelper.getWidths(scores, 2);
        return ok(
            trainresult.render(words, width1, width2, width3)
        );
      } else {
        return redirect(
            routes.Application.index());
      }
    }
    
    public static Result infer() {
      Form<InferRequest> inferForm = form(InferRequest.class);
      return ok(
          createinfer.render(inferForm)
      );
    }
    
    public static Result inferSave() {
      Form<InferRequest> inferForm = form(InferRequest.class).bindFromRequest();
      InferRequest inferRequest = inferForm.get();
      
      // call infer here
      Master master = Master.getInstance();
      try {
        master.infer(inferForm.get().document, "/Users/hadoop/project/plda/infer");
      } catch (IOException e) {
        e.printStackTrace();
        System.exit(-1);
      }
      
      return redirect(
          routes.Application.getInferResult()
      );
    }
    
    public static Result getInferResult() {
      Master master = Master.getInstance();
      InferResult iresult = null;
      try {
        iresult = master.getInferResult();
      } catch (NumberFormatException e) {
        e.printStackTrace();
        System.exit(-1);
      } catch (IOException e) {
        e.printStackTrace();
        System.exit(-1);
      }
      Map<String, ArrayList<Integer>> result = iresult.getWordEpicMap();
      final int[] docwidth = WidthHelper.getDocumentWidths(iresult.getInferOut());
      List<WordScore> scores = WidthHelper.getWordScores(result, 100);
      final String[] words = WidthHelper.getWords(scores);
      final int[] width1 = WidthHelper.getWidths(scores, 0);
      final int[] width2 = WidthHelper.getWidths(scores, 1);
      final int[] width3 = WidthHelper.getWidths(scores, 2);
      return ok(
          inferresult.render(words, docwidth, width1, width2, width3)
      );
    }
}
            
