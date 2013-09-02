package topic.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

public class FileListFinder {
  private String rootPath;
  private ArrayList<String> filePathList;
  
  public FileListFinder(String rootPath) {
    this.rootPath = rootPath;
    this.filePathList = new ArrayList<String>();
  }
  
  private void exe(File rootFile) {
    if (rootFile.isFile()) {
      this.filePathList.add(rootFile.getAbsolutePath());
    } else {
      File[] childArray = rootFile.listFiles();
      for (File child : childArray) {
        exe(child);
      }
    }
  }
  
  public ArrayList<String> getFilesList() {
    File rootFile = new File(this.rootPath);
    exe(rootFile);
    return this.filePathList;
  }
  
   
  public static void main(String[] args) throws IOException {
    String rootPath = "/Users/caoj7/data/training_doc_set";

    FileListFinder fileListTool = new FileListFinder(rootPath);
    ArrayList<String> fileList = fileListTool.getFilesList();
    
    BufferedWriter bw = new BufferedWriter(new FileWriter("list.txt"));
    for (String fileName : fileList) {
      bw.write(fileName);
      bw.flush();
      bw.newLine();
    }
    bw.close();
  }

}
