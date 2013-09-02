package topic;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import topic.util.CMDExecutor;
import topic.util.Dictionary;
import topic.util.HDFSTool;

public class Master {
  private Map<String, ArrayList<Integer>> dictionaryMap = null;
  private String infer_input = null;
  private String infer_output = null;
  
  private static Master instance = null;
  private boolean trainFinished;
  
  static public synchronized Master getInstance() {
    if (instance == null) {
      instance = new Master();
      instance.init();
      return instance;
    }
    return instance;
  }
  
  public void init() {
    File file = new File(TopicConstants.DOCSET_LOCAL_DIR);
    file.mkdirs();
    trainFinished = false;
  }
  
  public void train(String trainingDocPathInHDFS, String mpi_lda_path, int num_topics) throws IOException, InterruptedException {
    //1. download training-doc-set form HDFS
    // HDFSTool.download(trainingDocPathInHDFS, TopicConstants.DOCSET_LOCAL_DIR);
    //2. transform training-doc-set to <word, count> format as training's input
    WordCount wc = new WordCount(trainingDocPathInHDFS, TopicConstants.TRAIN_INPUT_FILE);
    wc.count();
    //3. traning with PLDA
    String training_cmd = "hamster -v -np 4 " + mpi_lda_path + 
                          " --num_topics " + num_topics + 
                          " --alpha 0.1 --beta 0.01" + 
                          " --training_data_file " + TopicConstants.TRAIN_INPUT_FILE +
                          " --model_file " + TopicConstants.DICTIONARY_MODEL +
                          " --total_iterations 150";
    System.out.println("training_cmd:" + training_cmd);
    
    CMDExecutor executor = new CMDExecutor();
    executor.run(training_cmd);
    //4. load the dictionary into dictionaryMap
    this.dictionaryMap = Dictionary.getWordMap(TopicConstants.DICTIONARY_MODEL);
    trainFinished = true;
  }
  
  public boolean getTrainFinished() {
    return trainFinished;
  }
  
  public Map<String, ArrayList<Integer>> getTrainingResult() {
    return this.dictionaryMap;
  } 
  
  public void infer(String fileContent, String infer_bin_path) throws IOException {
    infer_input = TopicConstants.INFER_INPUT;
    infer_output = TopicConstants.INFER_OUTPUT;
    if (this.dictionaryMap == null) {
      throw new IOException("dictionary is not in memory.");
    }
    //1. save the filecontent as a file
    BufferedWriter bw = new BufferedWriter(new FileWriter(TopicConstants.INFER_DOC));
    bw.write(fileContent);
    bw.flush();
    bw.close();
    //2.transform infer-file to <word, count> format as infer's input
    WordCount wc = new WordCount(TopicConstants.INFER_DOC, infer_input);
    wc.count(); 
    // 3. infer 
    String infer_cmd =  infer_bin_path + " --alpha 0.1 --beta 0.01" +
        " --inference_data_file " + infer_input +
        " --inference_result_file " + infer_output + 
        " --model_file " + TopicConstants.DICTIONARY_MODEL +
        " --total_iterations 15 --burn_in_iterations 10";

    CMDExecutor executor = new CMDExecutor();
    executor.run(infer_cmd);
  }
   
  private ArrayList<Double> getInferOut() throws NumberFormatException, IOException {
    ArrayList<Double> inferOut = new ArrayList<Double>();
    BufferedReader br = new BufferedReader(new FileReader(this.infer_output));
    String line = null;
    while(null != (line = br.readLine())) {
      String[] strArray = line.split(" ");
      for (String str : strArray) {
        if (!str.isEmpty()) {
          inferOut.add(Double.valueOf(str));
        }
      }
    }
    br.close();
    return inferOut;
  }
  
  
  private Map<String, ArrayList<Integer>> getInferResultWordEpicMap() throws IOException {
    Map<String, ArrayList<Integer>> result = new HashMap<String, ArrayList<Integer>>();
    
    //1. build infer_input file as a wordList
    ArrayList<String> wordList = new ArrayList<String>();
    BufferedReader br = new BufferedReader(new FileReader(this.infer_input));
    String line = null;
    while(null != (line = br.readLine())) {
      String[] strArray = line.split(" ");
      for (String str : strArray) {
        if(!str.startsWith("[0-9]") && !str.isEmpty()) {
          wordList.add(str);
        }
      }
    }
    br.close(); 
    
    //2. get the word's key from dictionaryMap
    for (String key : wordList) {
      ArrayList<Integer> value = this.dictionaryMap.get(key);
      if (value != null) {
        result.put(key, value);
      }
    }
    
    return result;
  }

  public InferResult getInferResult() throws NumberFormatException, IOException {
    //1. get inferOut
    ArrayList<Double> inferOut = getInferOut();
    //2. build wordEpicMap
    Map<String, ArrayList<Integer>> wordEpicMap = getInferResultWordEpicMap();
    //3. construct InferResult
    InferResult inferResult = new InferResult(inferOut, wordEpicMap);
    return inferResult;
  }
  
  public static void main(String[] args) throws IOException, InterruptedException {
    String trainingDocPathInHDFS = "/user/caoj7/training_doc_set";
    String mpi_lda_bin = "/Users/caoj7/workspace-c/plda/mpi_lda";
    int num_topics = 3;
    Master m = new Master();
    //1.
    m.init();
    //2. train
    m.train(trainingDocPathInHDFS, mpi_lda_bin, num_topics);
    //3. infer
    String inferFileContent = "It might be contended that if gun murderers were deprived of guns " +
                          " that they would find a way to kill as often with knives.  If this were " +        
                          " so, knife attacks in cities where guns were widely used in homicide";
    String infer_bin_path = "/Users/caoj7/workspace-c/plda/infer";
    m.infer(inferFileContent, infer_bin_path);
    //4. check infer result
    InferResult inferResult = m.getInferResult();
    ArrayList<Double> doubleList = inferResult.getInferOut();
    for (Double d : doubleList) {
      System.out.println(d);
    }
    System.out.println("------------------");
    Map<String, ArrayList<Integer>> wordEpicMap = inferResult.getWordEpicMap();
    for (Entry<String, ArrayList<Integer>> entry : wordEpicMap.entrySet()) {
      ArrayList<Integer> int2List = entry.getValue();
      String line = entry.getKey() + " ";
      for (Integer i : int2List) {
        line += i + " ";
      }
      System.out.println(line.trim());
    }
    
  }
}
