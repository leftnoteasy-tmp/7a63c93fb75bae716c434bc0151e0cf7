package topic;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import topic.util.FileListFinder;

public class WordCount {
  private String rootDirPath = null;
  private BufferedWriter bw = null;
  private ArrayList<String> fileFullPathList = null;
  private Set<String> stopWords = null;
  final String[] STOP_WORDS_ARRAY = new String[] {
      "a", "about", "above", "above", "across", "after", "afterwards", "again", "against", "all", "almost", "alone", "along", "already", "also","although","always","am","among", "amongst", "amoungst", "amount",  "an", "and", "another", "any","anyhow","anyone","anything","anyway", "anywhere", "are", "around", "as",  "at", "back","be","became", "because","become","becomes", "becoming", "been", "before", "beforehand", "behind", "being", "below", "beside", "besides", "between", "beyond", "bill", "both", "bottom","but", "by", "call", "can", "cannot", "cant", "co", "con", "could", "couldnt", "cry", "de", "describe", "detail", "do", "done", "down", "due", "during", "each", "eg", "eight", "either", "eleven","else", "elsewhere", "empty", "enough", "etc", "even", "ever", "every", "everyone", "everything", "everywhere", "except", "few", "fifteen", "fify", "fill", "find", "fire", "first", "five", "for", "former", "formerly", "forty", "found", "four", "from", "front", "full", "further", "get", "give", "go", "had", "has", "hasnt", "have", "he", "hence", "her", "here", "hereafter", "hereby", "herein", "hereupon", "hers", "herself", "him", "himself", "his", "how", "however", "hundred", "ie", "if", "in", "inc", "indeed", "interest", "into", "is", "it", "its", "itself", "keep", "last", "latter", "latterly", "least", "less", "ltd", "made", "many", "may", "me", "meanwhile", "might", "mill", "mine", "more", "moreover", "most", "mostly", "move", "much", "must", "my", "myself", "name", "namely", "neither", "never", "nevertheless", "next", "nine", "no", "nobody", "none", "noone", "nor", "not", "nothing", "now", "nowhere", "of", "off", "often", "on", "once", "one", "only", "onto", "or", "other", "others", "otherwise", "our", "ours", "ourselves", "out", "over", "own","part", "per", "perhaps", "please", "put", "rather", "re", "same", "see", "seem", "seemed", "seeming", "seems", "serious", "several", "she", "should", "show", "side", "since", "sincere", "six", "sixty", "so", "some", "somehow", "someone", "something", "sometime", "sometimes", "somewhere", "still", "such", "system", "take", "ten", "than", "that", "the", "their", "them", "themselves", "then", "thence", "there", "thereafter", "thereby", "therefore", "therein", "thereupon", "these", "they", "thickv", "thin", "third", "this", "those", "though", "three", "through", "throughout", "thru", "thus", "to", "together", "too", "top", "toward", "towards", "twelve", "twenty", "two", "un", "under", "until", "up", "upon", "us", "very", "via", "was", "we", "well", "were", "what", "whatever", "when", "whence", "whenever", "where", "whereafter", "whereas", "whereby", "wherein", "whereupon", "wherever", "whether", "which", "while", "whither", "who", "whoever", "whole", "whom", "whose", "why", "will", "with", "within", "without", "would", "yet", "you", "your", "yours", "yourself", "yourselves", "the"
  };

  public WordCount(String rootDirPath, String outputFilePath) {
    this.rootDirPath = rootDirPath;
    //get all file list
    FileListFinder fileListFinder = new FileListFinder(rootDirPath);
    this.fileFullPathList = fileListFinder.getFilesList();
    //prepare the BufferedWriter
    try {
      this.bw = new BufferedWriter(new FileWriter(outputFilePath, true));
    } catch (IOException e) {
      e.printStackTrace();
    }
    
    stopWords = new HashSet<String>();
    
    for (String sw : STOP_WORDS_ARRAY) {
      stopWords.add(sw);
    }
  }

  private void filterString(String originStr, StringBuffer sb) {
    originStr = originStr.trim();
    for (int i = 0; i < originStr.length(); i++) {
      char ch = originStr.charAt(i);
      if (ch > 64 && ch < 91 || ch > 96 && ch < 123) {
        sb.append(ch);
      } else {
        sb.append(" ");
      }
    }
  }
  
  private String[] filterWord(String[] before) {
    List<String> afterList = new ArrayList<String>();
    for (String s : before) {
      //remove 
      String trimed = s.trim().toLowerCase();
      if (trimed.isEmpty()) {
        continue;
      }
      if (trimed.length() <= 3) {
        continue;
      }
      if (stopWords.contains(trimed)) {
        continue;
      } else {
        afterList.add(trimed);
      }
    }
    
    return afterList.toArray(new String[0]);
  }
  
  public void count() throws IOException {
    for (String fileFullName : this.fileFullPathList) {
      Map<String, Integer> wordMap = new HashMap<String, Integer>();
      BufferedReader br = new BufferedReader(new FileReader(fileFullName));
      String line = null;
      while ((line = br.readLine()) != null) {
        if (line.isEmpty() || line.length() == 0) {
          continue;
        }
        
        if (line.contains("@")) {
          continue;
        }
        //toLowerCase
        line = line.toLowerCase();
        //filter the character which is not in the range 'a-z' and 'A-Z'
        StringBuffer filterSB = new StringBuffer();
        filterString(line, filterSB);
        String filteredLine = filterSB.toString();
//        System.out.println(filteredLine);
        
        //filter " " and "to, of ...."
        String[] wordArray = filteredLine.split(" ");
        String[] filteredWordArray = filterWord(wordArray);
        for (String aWord : filteredWordArray) {
          aWord = aWord.toLowerCase();
          if (wordMap.containsKey(aWord)) {
            wordMap.put(aWord,Integer.valueOf(wordMap.get(aWord).intValue() + 1));
          } else {
            wordMap.put(aWord, Integer.valueOf(1));
          }
        }
      }
      br.close();
      // write the resut *wordMap* to output file
      StringBuffer sb = new StringBuffer();
      Set<Entry<String, Integer>> set = wordMap.entrySet();
      for (Entry<String, Integer> entry : set) {
        sb.append(entry.getKey()).append(" ").append(entry.getValue()).append(" ");
      }

      String toLine = sb.toString();
      this.bw.write(toLine);
      this.bw.newLine();
      this.bw.flush();
    }
    this.bw.close();
  }

  
 
  /**
   * @param args
   * @throws IOException
   */
  public static void main(String[] args) throws IOException {    
    String rootPath = "/Users/caoj7/data/training_doc_set";
    String outputFilePath = "/tmp/wordcount.txt";
    WordCount wc = new WordCount(rootPath, outputFilePath);
    wc.count();
  }
}
