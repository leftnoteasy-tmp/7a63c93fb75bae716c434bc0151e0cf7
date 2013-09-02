package math;

import java.util.ArrayList;
import java.util.List;

public class WordScore {
  String word;
  List<Integer> scores;
  
  public WordScore(String word, List<Integer> scores) {
    this.word = word;
    this.scores = scores;
  }
  
  public WordScore(String word, int[] s) {
    this.word = word;
    this.scores = new ArrayList<Integer>();
    for (int i = 0; i < s.length; i++) {
      scores.add(s[i]);
    }
  }
  
  public String getWord() {
    return word;
  }
  
  public List<Integer> getScores() {
    return scores;
  }
  
  public int getSum() {
    int sum = 0;
    for (int i = 0; i < scores.size(); i++) {
      sum += scores.get(i);
    }
    return sum;
  }
  
  public int getMax() {
    int max = -1;
    for (int i = 0; i < scores.size(); i++) {
      if (scores.get(i) > max) {
        max = scores.get(i);
      }
    }
    return max;
  }
  
  public float getSD() {
    if (getSum() == 0) {
      return 0f;
    }
    float avg = getSum() / (float)scores.size();
    float sds = 0;
    for (int i = 0; i < scores.size(); i++) {
      sds += (scores.get(i) - avg) * (scores.get(i) - avg);
    }
    return (float) Math.sqrt(sds);
  }
  
  public static List<Double> getMockDocumentScores() {
    List<Double> arr = new ArrayList<Double>();
    arr.add(3.99);
    arr.add(101.7);
    arr.add(83.73);
    return arr;
  }
  
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(word);
    for (int i = 0; i < scores.size(); i++) {
      sb.append(" ");
      sb.append(scores.get(i));
    }
    return sb.toString();
  }
  
  public static List<WordScore> getWordScores() {
    List<WordScore> arr = new ArrayList<WordScore>();
    arr.add(new WordScore("hello", new int[] { 33, 66, 99 }));
    arr.add(new WordScore("hello1", new int[] { 313, 66, 99 }));
    arr.add(new WordScore("hello2", new int[] { 33, 1234, 99 }));
    arr.add(new WordScore("hello3", new int[] { 33, 66, 99 }));
    arr.add(new WordScore("hello4", new int[] { 33, 6, 99 }));
    arr.add(new WordScore("hello5", new int[] { 33, 66, 99 }));
    arr.add(new WordScore("hello6", new int[] { 123, 66, 8876 }));
    arr.add(new WordScore("hello7", new int[] { 33, 66, 99 }));
    arr.add(new WordScore("hello8", new int[] { 33, 66, 99 }));
    arr.add(new WordScore("hello9", new int[] { 33, 112, 99 }));
    arr.add(new WordScore("hello10", new int[] { 1123, 66, 99 }));
    arr.add(new WordScore("hello11", new int[] { 33, 66, 99 }));
    arr.add(new WordScore("hello12", new int[] { 33, 6612, 99 }));
    arr.add(new WordScore("hello13", new int[] { 33, 66, 99 }));
    arr.add(new WordScore("hello14", new int[] { 1233, 66, 99 }));
    arr.add(new WordScore("hello15", new int[] { 33, 222, 99 }));
    arr.add(new WordScore("hello16", new int[] { 33, 66, 99 }));
    arr.add(new WordScore("hello17", new int[] { 33, 7875, 99 }));
    arr.add(new WordScore("hello18", new int[] { 33, 66, 99 }));
    arr.add(new WordScore("hello19", new int[] { 3312, 66, 99 }));
    arr.add(new WordScore("hello20", new int[] { 33, 6623, 99 }));
    arr.add(new WordScore("hello21", new int[] { 33, 66, 99 }));
    arr.add(new WordScore("hello22", new int[] { 3233, 66, 9669 }));
    arr.add(new WordScore("hello23", new int[] { 33, 6, 99 }));
    arr.add(new WordScore("hello24", new int[] { 33, 66, 99 }));
    arr.add(new WordScore("hello25", new int[] { 3, 66, 99 }));
    arr.add(new WordScore("hello26", new int[] { 33, 66, 9 }));
    arr.add(new WordScore("hello27", new int[] { 0, 0, 0 }));
    return arr;
  }
}
