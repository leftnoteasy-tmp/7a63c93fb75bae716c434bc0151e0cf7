package com.pivotal.gibbs.cli;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ClientRMProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.util.ConverterUtils;

import com.pivotal.gibbs.cli.common.YarnClientConfig;
import com.pivotal.gibbs.cli.common.YarnClientException;

public class YarnCli {
  private static final Log LOG = LogFactory.getLog(YarnCli.class);
  private final String DEFAULT_MLAAS_DIR_IN_HDFS = "gibbs-yarncli";
  
  ApplicationId appId;
  Configuration conf;
  ClientRMProtocol client;
  RecordFactory recordFactory;
  
  public YarnCli() {
    this.conf = new YarnConfiguration();
    recordFactory = RecordFactoryProvider.getRecordFactory(null);
    
    initialize();
  }
  
  void initialize() {
    createYarnRPC();
  }
  
  public void doSubmit() throws YarnRemoteException, IOException, YarnClientException, InterruptedException {
    getNewApplication();
    
    submitApplication();
  }
  
  void setContainerCtxResource(ContainerLaunchContext ctx) {
    Resource resource = recordFactory.newRecordInstance(Resource.class);
    
    // we will use user specified memory to mpirun, by default, we will use 1024M
    resource.setMemory(YarnClientConfig.DEFAULT_MEM);
    
    // set virtual cores, by default we will use 1 core
    resource.setVirtualCores(YarnClientConfig.DEFAULT_CPU);
    
    ctx.setResource(resource);
  }
  
  FileSystem getRemoteFileSystem() throws IOException {
    return FileSystem.get(conf);
  }
  
  LocalResource constructLocalResource(FileSystem fs, String dirInHDFS,
      String filenameInHDFS, LocalResourceType type,
      LocalResourceVisibility visibility) throws IOException {
    LocalResource res = recordFactory.newRecordInstance(LocalResource.class);
    Path path = new Path(dirInHDFS, filenameInHDFS);
    FileStatus fsStatus = fs.getFileStatus(path);
    res.setResource(ConverterUtils.getYarnUrlFromPath(fsStatus.getPath()));
    res.setSize(fsStatus.getLen());
    res.setTimestamp(fsStatus.getModificationTime());
    res.setType(type);
    res.setVisibility(visibility);
    return res;
  }
  
  LocalResource constructLocalResource(FileSystem fs, String dirInHDFS,
      String fileNameInHDFS, LocalResourceType type) throws IOException {
    return constructLocalResource(fs, dirInHDFS, fileNameInHDFS, type,
        LocalResourceVisibility.APPLICATION);
  }
  
  String uploadFileToHDFS(String fullPath, FileSystem fs, String dirInHDFS) throws IOException {
    //parse fullPath to obtain localFilePath and fileName
    String localFilePath = null;
    String fileName = null;
    if (-1 != fullPath.indexOf('#')) {
      String[] splitPath = fullPath.split("#");
      localFilePath = splitPath[0];
      fileName = splitPath[1];
    } else {
      localFilePath = fullPath;
      File f = new File(localFilePath);
      fileName = f.getName();
    }
    
    //upload local file to HDFS
    Path filePathInHDFS = new Path(dirInHDFS, fileName);

    FSDataOutputStream os = fs.create(filePathInHDFS);
    BufferedInputStream bis = new BufferedInputStream(new FileInputStream(localFilePath));
    byte[] buffer = new byte[1024];
    int len = 0;
    while (-1 != (len = bis.read(buffer))) {
      os.write(buffer, 0, len);
    }
    os.flush();
    os.close();
    bis.close();
    return fileName;
  }
 
  void setContainerCtxLocalResources(ContainerLaunchContext ctx) throws IOException {
    Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
    
    // if the dirInHDFS does not exists, we will create it on HDFS
    String uploadRootPath = DEFAULT_MLAAS_DIR_IN_HDFS;
    FileSystem fs = getRemoteFileSystem();
    
    // upload path for this app
    Path appUploadPath = new Path(uploadRootPath, "app_upload_" + appId.getClusterTimestamp() + "_" + appId.getId());
    
    // normalize app upload path if it's not absolute path
    if (!appUploadPath.isAbsolute()) {
      appUploadPath = new Path(fs.getHomeDirectory().toString() + "/" + appUploadPath.toString());
    }
    
    if (!fs.exists(appUploadPath)) {
      fs.mkdirs(appUploadPath);
    }
    
    //obtain archive from archiveList one by one, and then upload it to HDFS
    String fullPath = System.getenv("APP_ENGINE_ARCHIVE_PATH");
    String archiveNameInHDFS = uploadFileToHDFS(fullPath, fs, appUploadPath.toString());
    LocalResource res = constructLocalResource(fs, appUploadPath.toString(), archiveNameInHDFS, LocalResourceType.ARCHIVE);
    //remove postfix from archive file name as the key
    String key = null;
    if (archiveNameInHDFS.endsWith(".tar.gz")) {
      key = archiveNameInHDFS.substring(0, archiveNameInHDFS.indexOf(".tar.gz"));
    } else if (archiveNameInHDFS.endsWith(".zip")) {
      key = archiveNameInHDFS.substring(0, archiveNameInHDFS.indexOf(".zip"));
    } else if (archiveNameInHDFS.endsWith(".tar")) {
      key = archiveNameInHDFS.substring(0, archiveNameInHDFS.indexOf(".tar"));
    } else if (archiveNameInHDFS.endsWith(".tgz")) {
      key = archiveNameInHDFS.substring(0, archiveNameInHDFS.indexOf(".tgz"));
    } else {
      LOG.warn("the archive file not with ordinary name, use the whole file name as key:" + fullPath);
      key = new File(fullPath).getName();
    }
    localResources.put(key, res);
    
    ctx.setLocalResources(localResources);
  }
  
  void setContainerCtxCommand(ContainerLaunchContext ctx) {
    String command = "cd appengine-proxy-1.0/appengine-proxy-1.0/ && sh start -DapplyEvolutions.default=true " + "1>"
        + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout 2>"
        + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr";
    List<String> cmds = new ArrayList<String>();
    
    // set it to 
    cmds.add(command);
    ctx.setCommands(cmds);
  }
  
  ApplicationSubmissionContext createAppSubmissionCtx() throws IOException {
    // get application submission context
    ApplicationSubmissionContext val = recordFactory
        .newRecordInstance(ApplicationSubmissionContext.class);

    // get container launch context (for command, local resource, etc.)
    ContainerLaunchContext ctx = recordFactory
        .newRecordInstance(ContainerLaunchContext.class);
    
    // set app-id to app context
    val.setApplicationId(appId);
   
    // set resource spec to container launch context
    setContainerCtxResource(ctx);
    
    // set local resource 
    setContainerCtxLocalResources(ctx);

    // set env to ctx
    setContainerCtxEnvs(ctx);
    
    // set command for container launch context
    setContainerCtxCommand(ctx);
    
    // set application name
    val.setApplicationName("appengine");
    
    // set container launch context to app context
    val.setAMContainerSpec(ctx);
   
    return val;
  }
  
  void setContainerCtxEnvs(ContainerLaunchContext ctx) throws IOException {
    // copy env
    ctx.setEnvironment(System.getenv());
  }
  
  void createYarnRPC() {
    YarnRPC rpc = YarnRPC.create(conf);
    InetSocketAddress rmAddress = NetUtils.createSocketAddr(conf.get(
        YarnConfiguration.RM_ADDRESS, YarnConfiguration.DEFAULT_RM_ADDRESS));
    this.client = (ClientRMProtocol) (rpc.getProxy(ClientRMProtocol.class,
        rmAddress, conf));
  }
  
  void getNewApplication() throws IOException, YarnRemoteException {
    if (client == null) {
      throw new IOException("should initialize YARN client first.");
    }
    GetNewApplicationRequest request = recordFactory
        .newRecordInstance(GetNewApplicationRequest.class);
    GetNewApplicationResponse newAppResponse = client.getNewApplication(request);
    appId = newAppResponse.getApplicationId();
  }
  
  FinalApplicationStatus waitForApplicationTerminated() throws IOException, InterruptedException {
    // query request
    GetApplicationReportRequest reportRequest;
    ApplicationReport report;
    YarnApplicationState state;
    YarnApplicationState preState = YarnApplicationState.NEW;
    String trackingUrl = null;
    
    reportRequest = recordFactory.newRecordInstance(GetApplicationReportRequest.class);
    reportRequest.setApplicationId(appId);
    
    // poll RM, get AM state
    report = client.getApplicationReport(reportRequest).getApplicationReport();
    state = report.getYarnApplicationState();
    while (true) {
      report = client.getApplicationReport(reportRequest).getApplicationReport();
      preState = state;
      if (report.getTrackingUrl() != null && (!report.getTrackingUrl().isEmpty())) {
        if (trackingUrl == null && (report.getRpcPort() > 0)) {
          trackingUrl = report.getTrackingUrl();
          LOG.info("tracking url: http://" + report.getHost() + ":" + report.getRpcPort());
        }
      }
      state = report.getYarnApplicationState();
      
      // state changed
      if (preState != state) {
        LOG.info("yarn application state transfered from [" + preState.name() + "] to [" + state.name() + "]");
      }
      
      // application terminated
      if (state == YarnApplicationState.FAILED || state == YarnApplicationState.FINISHED || state == YarnApplicationState.KILLED) {
        break;
      }
      Thread.sleep(100);
    }
    
    FinalApplicationStatus finalStatus = report.getFinalApplicationStatus();
    if (finalStatus != FinalApplicationStatus.SUCCEEDED) {
      LOG.error("Final state of AppMaster is," + finalStatus.name());
    } else {
      LOG.info("AppMaster is successfully finished.");
    }
    
    return finalStatus;
  }
  
  ApplicationSubmissionContext submitApplication() throws YarnClientException,
      InterruptedException, IOException {
    if (client == null) {
      throw new YarnClientException("should initialize YARN client first.");
    }

    // submit application
    SubmitApplicationRequest submitRequest = recordFactory
        .newRecordInstance(SubmitApplicationRequest.class);
    ApplicationSubmissionContext submissionCtx = createAppSubmissionCtx();
    submitRequest.setApplicationSubmissionContext(submissionCtx);
    client.submitApplication(submitRequest);

    // wait for application get started
    waitForApplicationTerminated();
    
    return submissionCtx;
  }

  public static void main(String[] args) throws YarnClientException,
      YarnRemoteException, IOException, InterruptedException {
    YarnCli cli = new YarnCli();
    cli.doSubmit();
  }
}
