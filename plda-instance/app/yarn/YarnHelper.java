package yarn;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.PrivilegedAction;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.api.AMRMProtocol;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ClientRMProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.YarnRPC;

public class YarnHelper {
  private static final Log LOG = LogFactory.getLog(YarnHelper.class);
  private static YarnHelper instance = null;
  
  Configuration conf;
  RecordFactory recordFactory;
  ClientRMProtocol client;
  ApplicationId appId;
  AMRMProtocol scheduler;
  
  public synchronized static YarnHelper getInstance() {
    if (null == instance) {
      return new YarnHelper();
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
      ApplicationAttemptId attemptId = recordFactory.newRecordInstance(ApplicationAttemptId.class);
      attemptId.setApplicationId(appId);
      attemptId.setAttemptId(1);
      request.setApplicationAttemptId(attemptId);
      RegisterApplicationMasterResponse response = scheduler.registerApplicationMaster(request);
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
    
    // register To RM
    registerToRM();
  }
}
