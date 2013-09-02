package topic.util;

import java.io.IOException;

public class HDFSTool {
  public static final void download(String pathInHDFS, String localPath) throws IOException, InterruptedException {
    String download_cmd = "hadoop fs -get " + pathInHDFS + " " + localPath;
    System.out.println(download_cmd);
    Process proc = Runtime.getRuntime().exec(download_cmd);
    int exitCode = proc.waitFor();
    if (0 != exitCode) {
      throw new IOException("fs download failed.");
    }
  }
}
