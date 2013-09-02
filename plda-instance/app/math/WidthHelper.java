package math;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

public class WidthHelper {
  static final int MAX_WIDTH = 80;
  
  public static int[] getDocumentWidths(List<Double> widths) {
   for (int i = 0; i < widths.size(); i++) {
     System.out.println("doc:" + widths.get(i));
   }
   double max = -1;
   for (double w : widths) {
     if (w > max) {
       max = w;
     }
   }
   if (max == 0) {
     int[] result = new int[widths.size()];
     for (int i = 0; i < widths.size(); i++) {
       result[i] = 0;
     }
     return result;
   }
   float perWidth = (float) (max / MAX_WIDTH);
   int[] results = new int[widths.size()];
   for (int i = 0; i < widths.size(); i++) {
     results[i] = (int) Math.round(widths.get(i) / perWidth);
     if (results[i] <= 0) {
       results[i] = 1;
     }
   }
   
   return results;
  }
  
  public static List<WordScore> getWordScores(Map<String, ArrayList<Integer>> scoreMap, int maxCount) {
    List<WordScore> wordScores = new ArrayList<WordScore>();
    
    for (Map.Entry<String, ArrayList<Integer>> e : scoreMap.entrySet()) {
      ArrayList<Integer> copy = new ArrayList<Integer>();
      for (int i : e.getValue()) {
        copy.add(i);
      }
      WordScore ws = new WordScore(e.getKey(), copy);
      wordScores.add(ws);
    }
    
    // we will sort wordScores
    Collections.sort(wordScores, new Comparator<WordScore>() {

      @Override
      public int compare(WordScore left, WordScore right) {
//        float result = right.getSD() - left.getSD();
//        if (result > 1e-8) {
//          return 1;
//        } else if (result < -1e-8) {
//          return -1;
//        }
//        return 0;
        return right.getMax() - left.getMax();
      }
      
    });
    
    List<WordScore> result = new ArrayList<WordScore>();
    for (int i = 0; i < Math.min(wordScores.size(), maxCount); i++) {
      System.out.println(wordScores.get(i).toString());
      result.add(wordScores.get(i));
    }
    
    normalizeScores(result);
    
    return result;
  }
  
  public static void normalizeScores(List<WordScore> scores) {
    for (int i = 0; i < scores.size(); i++) {
      WordScore ws = scores.get(i);
      int sum = ws.getSum();
      if (sum == 0) {
        continue;
      }
      float perWidth = sum / (float)MAX_WIDTH;
      for (int j = 0; j < ws.getScores().size(); j++) {
        int score = ws.getScores().get(j);
        score = Math.round(score / perWidth);
        if (score <= 0) {
          score = 1;
        }
        ws.getScores().set(j, score);
      }
    }
  }
  
  public static String[] getWords(List<WordScore> ws) {
    String[] result = new String[ws.size()];
    for (int i = 0; i < ws.size(); i++) {
      result[i] = ws.get(i).word;
    }
    return result;
  }
  
  public static int[] getWidths(List<WordScore> scores, int k) {

    
    int[] result = new int[scores.size()];
    for (int i = 0; i < scores.size(); i++) {
      result[i] = scores.get(i).getScores().get(k);
    }
    return result;
  }
}
