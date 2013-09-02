package topic;

public class TopicConstants {
  public static final String TIME = String.valueOf(System.currentTimeMillis());
  public static final String LOCAL_TMP_PATH = "/tmp/topic_" +TIME;
  
  
  public static final String DOCSET_LOCAL_DIR = LOCAL_TMP_PATH + "/training_doc_set";
  public static final String TRAIN_INPUT_FILE = LOCAL_TMP_PATH + "/train.input.wordcount";
  public static final String DICTIONARY_MODEL = LOCAL_TMP_PATH + "/dictionary.model";
  
  public static final String INFER_DOC = LOCAL_TMP_PATH + "/infer.raw.doc." + System.currentTimeMillis();
  public static final String INFER_INPUT = LOCAL_TMP_PATH + "/infer.input.wordcount." + System.currentTimeMillis();
  public static final String INFER_OUTPUT = LOCAL_TMP_PATH + "/infer.output." + System.currentTimeMillis();
  
}
