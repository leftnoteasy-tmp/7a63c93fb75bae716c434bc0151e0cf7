package topic.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;

public class CMDExecutor {
  private static boolean errMark = false;
  
//-----------------------
  private static class StreamGobbler implements Runnable {
    private BufferedReader reader;
    private boolean out;
    
    public StreamGobbler(BufferedReader reader, boolean out) {
      this.reader = reader;
      this.out = out;
    }

    public void run() {
      try {
        String line = null;
        while ((line = reader.readLine()) != null) {
          if (out) {
            System.out.println(line);
          } else {
            System.err.println(line);
          }
        }
      } catch (IOException e) {
        e.printStackTrace();
        errMark = true;
      }
    }
  }
  //-----------------------
  
  public void run(String cmd) {
    Process proc = null;
    try {
      proc = Runtime.getRuntime().exec(cmd);
      proc.waitFor();
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    
    // get err stream and out stream
    BufferedReader bre = new BufferedReader(new InputStreamReader(proc.getErrorStream()));
    BufferedReader bri = new BufferedReader(new InputStreamReader(proc.getInputStream()));

    // use thread fetch output
    Thread errThread = new Thread(new StreamGobbler(bre, false));
    Thread outThread = new Thread(new StreamGobbler(bri, true));
    
    errThread.start();
    outThread.start();
    
    // wait for thread die
    try {
      errThread.join();
      outThread.join();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    
    try {
      bri.close();
      bre.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
  
  public static void main(String[] args) throws IOException,
      InterruptedException {
    String cmd = "hamster -v -np 4 /Users/caoj7/program/mpi/hello";
    CMDExecutor executor = new CMDExecutor();
    executor.run(cmd);
    
  }

}