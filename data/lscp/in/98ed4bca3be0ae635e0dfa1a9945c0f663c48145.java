hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-web-proxy/src/main/java/org/apache/hadoop/yarn/server/webproxy/AppReportFetcher.java
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.ApplicationHistoryProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.client.AHSProxy;
import org.apache.hadoop.yarn.client.ClientRMProxy;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.ApplicationNotFoundException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.factories.RecordFactory;
  private static final Log LOG = LogFactory.getLog(AppReportFetcher.class);
  private final Configuration conf;
  private final ApplicationClientProtocol applicationsManager;
  private final ApplicationHistoryProtocol historyManager;
  private final RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);
  private boolean isAHSEnabled;

  public AppReportFetcher(Configuration conf) {
    if (conf.getBoolean(YarnConfiguration.APPLICATION_HISTORY_ENABLED,
        YarnConfiguration.DEFAULT_APPLICATION_HISTORY_ENABLED)) {
      isAHSEnabled = true;
    }
    this.conf = conf;
    try {
      applicationsManager = ClientRMProxy.createRMProxy(conf,
          ApplicationClientProtocol.class);
      if (isAHSEnabled) {
        historyManager = getAHSProxy(conf);
      } else {
        this.historyManager = null;
      }
    } catch (IOException e) {
      throw new YarnRuntimeException(e);
    }
  }
  
  public AppReportFetcher(Configuration conf, ApplicationClientProtocol applicationsManager) {
    if (conf.getBoolean(YarnConfiguration.APPLICATION_HISTORY_ENABLED,
        YarnConfiguration.DEFAULT_APPLICATION_HISTORY_ENABLED)) {
      isAHSEnabled = true;
    }
    this.conf = conf;
    this.applicationsManager = applicationsManager;
    if (isAHSEnabled) {
      try {
        historyManager = getAHSProxy(conf);
      } catch (IOException e) {
        throw new YarnRuntimeException(e);
      }
    } else {
      this.historyManager = null;
    }
  }

  protected ApplicationHistoryProtocol getAHSProxy(Configuration configuration)
      throws IOException {
    return AHSProxy.createAHSProxy(configuration,
      ApplicationHistoryProtocol.class,
      configuration.getSocketAddr(YarnConfiguration.TIMELINE_SERVICE_ADDRESS,
        YarnConfiguration.DEFAULT_TIMELINE_SERVICE_ADDRESS,
        YarnConfiguration.DEFAULT_TIMELINE_SERVICE_PORT));
  }

        .newRecordInstance(GetApplicationReportRequest.class);
    request.setApplicationId(appId);

    GetApplicationReportResponse response;
    try {
      response = applicationsManager.getApplicationReport(request);
    } catch (YarnException e) {
      if (!isAHSEnabled) {
        throw e;
      }
      if (!(e.getClass() == ApplicationNotFoundException.class)) {
        throw e;
      }
      response = historyManager.getApplicationReport(request);
    }
    return response.getApplicationReport();
  }

    if (this.applicationsManager != null) {
      RPC.stopProxy(this.applicationsManager);
    }
    if (this.historyManager != null) {
      RPC.stopProxy(this.historyManager);
    }
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-web-proxy/src/test/java/org/apache/hadoop/yarn/server/webproxy/TestAppReportFetcher.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-web-proxy/src/test/java/org/apache/hadoop/yarn/server/webproxy/TestAppReportFetcher.java

package org.apache.hadoop.yarn.server.webproxy;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.ApplicationHistoryProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.ApplicationNotFoundException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class TestAppReportFetcher {

  static ApplicationHistoryProtocol historyManager;
  static Configuration conf = new Configuration();
  private static ApplicationClientProtocol appManager;
  private static AppReportFetcher fetcher;
  private final String appNotFoundExceptionMsg = "APP NOT FOUND";

  @After
  public void cleanUp() {
    historyManager = null;
    appManager = null;
    fetcher = null;
  }

  public void testHelper(boolean isAHSEnabled)
      throws YarnException, IOException {
    conf.setBoolean(YarnConfiguration.APPLICATION_HISTORY_ENABLED,
        isAHSEnabled);
    appManager = Mockito.mock(ApplicationClientProtocol.class);
    Mockito.when(appManager
        .getApplicationReport(Mockito.any(GetApplicationReportRequest.class)))
        .thenThrow(new ApplicationNotFoundException(appNotFoundExceptionMsg));
    fetcher = new AppReportFetcherForTest(conf, appManager);
    ApplicationId appId = ApplicationId.newInstance(0,0);
    fetcher.getApplicationReport(appId);
  }

  @Test
  public void testFetchReportAHSEnabled() throws YarnException, IOException {
    testHelper(true);
    Mockito.verify(historyManager, Mockito.times(1))
    .getApplicationReport(Mockito.any(GetApplicationReportRequest.class));
    Mockito.verify(appManager, Mockito.times(1))
    .getApplicationReport(Mockito.any(GetApplicationReportRequest.class));
  }

  @Test
  public void testFetchReportAHSDisabled() throws YarnException, IOException {
    try {
      testHelper(false);
    } catch (ApplicationNotFoundException e) {
      Assert.assertTrue(e.getMessage() == appNotFoundExceptionMsg);
    }
    Mockito.verify(appManager, Mockito.times(1))
    .getApplicationReport(Mockito.any(GetApplicationReportRequest.class));
    if (historyManager != null) {
      Assert.fail("HistoryManager should be null as AHS is disabled");
    }
  }

  static class AppReportFetcherForTest extends AppReportFetcher {

    public AppReportFetcherForTest(Configuration conf,
        ApplicationClientProtocol acp) {
      super(conf, acp);
    }

    @Override
    protected ApplicationHistoryProtocol getAHSProxy(Configuration conf)
        throws IOException
    {
      GetApplicationReportResponse resp = Mockito.
          mock(GetApplicationReportResponse.class);
      historyManager = Mockito.mock(ApplicationHistoryProtocol.class);
      try {
        Mockito.when(historyManager.getApplicationReport(Mockito
            .any(GetApplicationReportRequest.class))).thenReturn(resp);
      } catch (YarnException e) {
        e.printStackTrace();
      }
      return historyManager;
    }
  }
}

