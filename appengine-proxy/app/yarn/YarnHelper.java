package yarn;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import models.App;
import models.AppState;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.api.AMRMProtocol;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ClientRMProtocol;
import org.apache.hadoop.yarn.api.ContainerManager;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerToken;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.ProtoUtils;

public class YarnHelper {
  private static final Log LOG = LogFactory.getLog(YarnHelper.class);
  private static YarnHelper instance = null;
  private static String FILE_UPLOAD_DIR = "app-engine";
  
  Configuration conf;
  RecordFactory recordFactory;
  ClientRMProtocol client;
  ApplicationId appId;
  AMRMProtocol scheduler;
  ApplicationAttemptId attemptId;
  
  public synchronized static YarnHelper getInstance() {
    if (null == instance) {
      instance = new YarnHelper();
      return instance;
    }
    return instance;
  }

  public YarnHelper() {
    conf = new YarnConfiguration();
    recordFactory = RecordFactoryProvider.getRecordFactory(null);
    createYarnRPC();
  }
  
  void createYarnRPC() {
    YarnRPC rpc = YarnRPC.create(conf);
    InetSocketAddress rmAddress = NetUtils.createSocketAddr(conf.get(
        YarnConfiguration.RM_ADDRESS, YarnConfiguration.DEFAULT_RM_ADDRESS));
    this.client = (ClientRMProtocol) (rpc.getProxy(ClientRMProtocol.class,
        rmAddress, conf));
  }
  
  void getNewApplication() throws IOException {
    if (client == null) {
      throw new IOException("should initialize YARN client first.");
    }
    GetNewApplicationRequest request = recordFactory
        .newRecordInstance(GetNewApplicationRequest.class);
    GetNewApplicationResponse newAppResponse = client.getNewApplication(request);
    appId = newAppResponse.getApplicationId();
    System.out.println("appid:" + appId.toString());
  }
  
  ApplicationSubmissionContext createAppSubmissionCtx() throws IOException {
    // get application submission context
    ApplicationSubmissionContext val = recordFactory
        .newRecordInstance(ApplicationSubmissionContext.class);

    val.setUnmanagedAM(true);
    val.setApplicationId(appId);
    
    ContainerLaunchContext ctx = recordFactory.newRecordInstance(ContainerLaunchContext.class);
    ctx.setUser(UserGroupInformation.getCurrentUser().getUserName());
    
    val.setAMContainerSpec(ctx);
    
    return val;
  }
  
  void submitApplication() throws InterruptedException, IOException {
    if (client == null) {
      throw new IOException("should initialize YARN client first.");
    }
    
    // submit application
    SubmitApplicationRequest submitRequest = recordFactory
        .newRecordInstance(SubmitApplicationRequest.class);
    ApplicationSubmissionContext submissionCtx = createAppSubmissionCtx();
    submitRequest.setApplicationSubmissionContext(submissionCtx);
    client.submitApplication(submitRequest);
    
    // query application until state changed
    GetApplicationReportRequest reportRequest = recordFactory.newRecordInstance(GetApplicationReportRequest.class);
    reportRequest.setApplicationId(appId);
    ApplicationReport report;
    YarnApplicationState state;
    
    while (true) {
      report = client.getApplicationReport(reportRequest).getApplicationReport();
      state = report.getYarnApplicationState();
      boolean cont = false;
      if (YarnApplicationState.NEW == state || YarnApplicationState.SUBMITTED == state) {
        cont = true;
      }
      if (!cont) {
        break;
      }
    }
    
    if (state != YarnApplicationState.ACCEPTED) {
      throw new IOException("error state detected");
    }
  }

  AMRMProtocol createSchedulerProxy() {
    final YarnRPC rpc = YarnRPC.create(conf);
    final InetSocketAddress serviceAddr = conf.getSocketAddr(
        YarnConfiguration.RM_SCHEDULER_ADDRESS,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_ADDRESS,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_PORT);

    UserGroupInformation currentUser;
    try {
      currentUser = UserGroupInformation.getCurrentUser();
    } catch (IOException e) {
      throw new YarnException(e);
    }

    if (UserGroupInformation.isSecurityEnabled()) {
      String tokenURLEncodedStr = System.getenv().get(
          ApplicationConstants.APPLICATION_MASTER_TOKEN_ENV_NAME);
      Token<? extends TokenIdentifier> token = new Token<TokenIdentifier>();

      try {
        token.decodeFromUrlString(tokenURLEncodedStr);
      } catch (IOException e) {
        throw new YarnException(e);
      }

      SecurityUtil.setTokenService(token, serviceAddr);
      if (LOG.isDebugEnabled()) {
        LOG.debug("AppMasterToken is " + token);
      }
      currentUser.addToken(token);
    }

    return currentUser.doAs(new PrivilegedAction<AMRMProtocol>() {
      @Override
      public AMRMProtocol run() {
        return (AMRMProtocol) rpc.getProxy(AMRMProtocol.class,
            serviceAddr, conf);
      }
    });
  }
  
  void registerToRM() {
    try {
      RegisterApplicationMasterRequest request = recordFactory.newRecordInstance(RegisterApplicationMasterRequest.class);
      attemptId = recordFactory.newRecordInstance(ApplicationAttemptId.class);
      attemptId.setApplicationId(appId);
      attemptId.setAttemptId(1);
      request.setApplicationAttemptId(attemptId);
      RegisterApplicationMasterResponse response = scheduler.registerApplicationMaster(request);
      System.out.println("######### response.max:" + response.getMaximumResourceCapability().getMemory());
    } catch (Exception e) {
      LOG.error("exception while registering:", e);
      throw new YarnException(e);
    }
  }
  
  public void registerAM() throws IOException, InterruptedException {
    getNewApplication();
    submitApplication();
    
    // get scheduler proxy
    scheduler = createSchedulerProxy();
    System.out.println("############## scheduler is :" + (scheduler == null));
    
    // register To RM
    registerToRM();
  }
  
  Container allocateResource() throws InterruptedException, IOException {
    System.out.println("appatemptid:" + attemptId.toString());

    // make our allocate request
    AllocateRequest request = recordFactory.newRecordInstance(AllocateRequest.class);
    request.setApplicationAttemptId(this.attemptId);
    ResourceRequest rr = recordFactory.newRecordInstance(ResourceRequest.class);
    rr.setHostName("*");
    Resource res = recordFactory.newRecordInstance(Resource.class);
    res.setMemory(1024);
    res.setVirtualCores(0);
    rr.setCapability(res);
    rr.setNumContainers(1);
    Priority pri = recordFactory.newRecordInstance(Priority.class);
    pri.setPriority(20);
    rr.setPriority(pri);
    List<ResourceRequest> requests = new ArrayList<ResourceRequest>();
    requests.add(rr);
    request.addAllAsks(requests);
    
    if (scheduler == null) {
      throw new IOException("scheduler is null");
    }
    
    List<Container> containers = scheduler.allocate(request).getAMResponse().getAllocatedContainers();
    
    while (containers == null || containers.size() == 0) {
      Thread.sleep(500);
      containers = scheduler.allocate(request).getAMResponse().getAllocatedContainers();
      System.out.println("allocate again ... ");
    }
    
    if (containers.size() > 0) {
      return containers.get(0);
    }
    
    throw new IOException("failed to allocate");
  }
  
  ContainerManager getCMProxy(ContainerId containerID,
      final String containerManagerBindAddr, ContainerToken containerToken)
      throws IOException {
    final YarnRPC rpc = YarnRPC.create(conf);
    final InetSocketAddress cmAddr = NetUtils
        .createSocketAddr(containerManagerBindAddr);

    UserGroupInformation user = UserGroupInformation.getCurrentUser();

    if (UserGroupInformation.isSecurityEnabled()) {
      Token<ContainerTokenIdentifier> token = ProtoUtils
          .convertFromProtoFormat(containerToken, cmAddr);
      // the user in createRemoteUser in this context has to be ContainerID
      user = UserGroupInformation.createRemoteUser(containerID.toString());
      user.addToken(token);
    }

    ContainerManager proxy = user
        .doAs(new PrivilegedAction<ContainerManager>() {
          @Override
          public ContainerManager run() {
            return (ContainerManager) rpc.getProxy(ContainerManager.class,
                cmAddr, conf);
          }
        });
    return proxy;
  }
  
  LocalResource createLocalResource(File archive) throws IOException {
    LocalResource res = recordFactory.newRecordInstance(LocalResource.class);
    res.setSize(archive.length());
    res.setTimestamp(archive.lastModified());
    res.setType(LocalResourceType.ARCHIVE);
    res.setVisibility(LocalResourceVisibility.PRIVATE);
    
    // upload file to HDFS
    FileSystem fs = FileSystem.get(conf);
    Path path = new Path(FILE_UPLOAD_DIR, String.valueOf(new Random(System.currentTimeMillis()).nextLong()));
    fs.mkdirs(path);
    
    // file path
    path = new Path(path, archive.getName());
    FSDataOutputStream fsOut = fs.create(path);
    FileInputStream fis = new FileInputStream(archive);
    
    // read data from fis to fsOut
    byte[] buffer = new byte[1024];
    int len;
    while ((len = fis.read(buffer)) >= 0) {
      fsOut.write(buffer, 0, len);
    }
    fsOut.flush();
    fsOut.close();
    
    res.setResource(ConverterUtils.getYarnUrlFromPath(path));
    return res;
  }
  
  StartContainerRequest createStartContainerRequest(Container container, File archive, int port) throws IOException {
    StartContainerRequest request = recordFactory.newRecordInstance(StartContainerRequest.class);
    
    // create and set launch context
    ContainerLaunchContext launchCtx = recordFactory.newRecordInstance(ContainerLaunchContext.class);
    List<String> cmd = new ArrayList<String>();
    
    // set command
    cmd.add("HAHAH " + "1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr");
    
    // set container-id
    launchCtx.setContainerId(container.getId());
    
    // set environment
    launchCtx.setEnvironment(System.getenv());
    
    // set local resource
    LocalResource res = createLocalResource(archive);
    Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
    localResources.put("app", res);
    launchCtx.setLocalResources(localResources);
    
    // set resource will be used
    launchCtx.setResource(container.getResource());
    
    request.setContainerLaunchContext(launchCtx);
    
    return request;
  }
  
  void launchContainer(Container container, int port) throws IOException, InterruptedException {
    String appArchivePath = System.getenv("APP_ARCHIVE_PATH");
    if (null == appArchivePath) {
      throw new IOException("get app archive path failed");
    }
    File file = new File(appArchivePath);
    if (!file.exists()) {
      throw new IOException("app archive file not exist:" + appArchivePath);
    }
    
    ContainerManager cm = getCMProxy(container.getId(), container
        .getNodeId().getHost() + ":" + container.getNodeId().getPort(),
        container.getContainerToken());
    StartContainerRequest req = createStartContainerRequest(container, file, port);
    cm.startContainer(req);
    
    GetContainerStatusRequest queryRequest = recordFactory.newRecordInstance(GetContainerStatusRequest.class);
    queryRequest.setContainerId(container.getId());
    
    GetContainerStatusResponse response = cm.getContainerStatus(queryRequest);
    while (response.getStatus().getState() == ContainerState.NEW) {
      Thread.sleep(200);
      response = cm.getContainerStatus(queryRequest);
      System.out.println("get container status...");
    }
    
    if (response.getStatus().getState() == ContainerState.COMPLETE) {
      throw new IOException("container is completed, what happend?");
    }
  }

  public synchronized void runNewApp(App app) throws InterruptedException, IOException {
    int port = new Random(System.currentTimeMillis()).nextInt() % 10000 + 50000;
    Container allocatedContainer = allocateResource();
    launchContainer(allocatedContainer, port);
    app.state = AppState.Running;
    app.url = "http://" + allocatedContainer.getNodeId().getHost() + ":" + port;
  }
}
