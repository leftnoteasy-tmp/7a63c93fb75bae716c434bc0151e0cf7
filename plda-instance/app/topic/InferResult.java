package topic;

import java.util.ArrayList;
import java.util.Map;

public class InferResult {
  private ArrayList<Double> inferOut = null;
  private Map<String, ArrayList<Integer>> wordEpicMap = null;
  
  public InferResult(ArrayList<Double> inferOut, Map<String, ArrayList<Integer>> wordEpicMap) {
    this.inferOut = inferOut;
    this.wordEpicMap = wordEpicMap;
  }
  
  public ArrayList<Double> getInferOut() {
    return this.inferOut;
  }
  
  public Map<String, ArrayList<Integer>> getWordEpicMap() {
    return wordEpicMap;
  }
}
