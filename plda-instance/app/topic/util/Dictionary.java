package topic.util;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class Dictionary {
  public static final Map<String, ArrayList<Integer>> getWordMap(String dictionaryPath) {
    Map<String, ArrayList<Integer>>  result = new HashMap<String, ArrayList<Integer>>();
    try {
      BufferedReader br = new BufferedReader(new FileReader(dictionaryPath));
      String line = null;
      while (null != (line = br.readLine())) {
        String[] strArray = line.split("\t");
        String[] intStrArray = strArray[1].split(" ");
        ArrayList<Integer> intList = new ArrayList<Integer>();
        intList.add(Integer.valueOf(intStrArray[0]));
        intList.add(Integer.valueOf(intStrArray[1]));
        intList.add(Integer.valueOf(intStrArray[2]));
        result.put(strArray[0].trim(), intList);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    
    return result;
  }

  public static void main(String[] args) {
    String dictionaryPath = "/tmp/topic_1378103724845/dictionary.model";
    Map<String, ArrayList<Integer>> result = Dictionary.getWordMap(dictionaryPath);
    for (Entry<String, ArrayList<Integer>> entry : result.entrySet()) {
      ArrayList<Integer> intList = entry.getValue();
      String line = entry.getKey() + " ";
      for (Integer i : intList) {
        line += i + " ";
      }
      System.out.println(line.trim());
    }
  }

}
