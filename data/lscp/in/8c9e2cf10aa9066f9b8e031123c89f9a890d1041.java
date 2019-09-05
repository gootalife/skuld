hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-hs/src/main/java/org/apache/hadoop/mapreduce/v2/hs/server/HSAdminServer.java

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.PrivilegedExceptionAction;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.v2.hs.proto.HSAdminRefreshProtocolProtos.HSAdminRefreshProtocolService;
import org.apache.hadoop.mapreduce.v2.hs.protocolPB.HSAdminRefreshProtocolServerSideTranslatorPB;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.BlockingService;

@Private
  private static final String HISTORY_ADMIN_SERVER = "HSAdminServer";
  private JobHistory jobHistoryService = null;

  private UserGroupInformation loginUGI;

  public HSAdminServer(AggregatedLogDeletionService aggLogDelService,
      JobHistory jobHistoryService) {
    super(HSAdminServer.class.getName());

  @Override
  protected void serviceStart() throws Exception {
    if (UserGroupInformation.isSecurityEnabled()) {
      loginUGI = UserGroupInformation.getLoginUser();
    } else {
      loginUGI = UserGroupInformation.getCurrentUser();
    }
    clientRpcServer.start();
  }

  @VisibleForTesting
  UserGroupInformation getLoginUGI() {
    return loginUGI;
  }

  @VisibleForTesting
  void setLoginUGI(UserGroupInformation ugi) {
    loginUGI = ugi;
  }

  @Override
  protected void serviceStop() throws Exception {
    if (clientRpcServer != null) {
  public void refreshLogRetentionSettings() throws IOException {
    UserGroupInformation user = checkAcls("refreshLogRetentionSettings");

    try {
      loginUGI.doAs(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws IOException {
          aggLogDelService.refreshLogRetentionSettings();
          return null;
        }
      });
    } catch (InterruptedException e) {
      throw new IOException(e);
    }

    HSAuditLogger.logSuccess(user.getShortUserName(),
        "refreshLogRetentionSettings", "HSAdminServer");
  public void refreshJobRetentionSettings() throws IOException {
    UserGroupInformation user = checkAcls("refreshJobRetentionSettings");

    try {
      loginUGI.doAs(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws IOException {
          jobHistoryService.refreshJobRetentionSettings();
          return null;
        }
      });
    } catch (InterruptedException e) {
      throw new IOException(e);
    }

    HSAuditLogger.logSuccess(user.getShortUserName(),
        "refreshJobRetentionSettings", HISTORY_ADMIN_SERVER);

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-hs/src/test/java/org/apache/hadoop/mapreduce/v2/hs/server/TestHSAdminServer.java
import static org.junit.Assert.*;

import java.io.IOException;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
    verify(jobHistoryService).refreshJobRetentionSettings();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testUGIForLogAndJobRefresh() throws Exception {
    UserGroupInformation ugi =
        UserGroupInformation.createUserForTesting("test", new String[] {"grp"});
    UserGroupInformation loginUGI = spy(hsAdminServer.getLoginUGI());
    hsAdminServer.setLoginUGI(loginUGI);

    ugi.doAs(new PrivilegedAction<Void>() {
      @Override
      public Void run() {
        String[] args = new String[1];
        args[0] = "-refreshLogRetentionSettings";
        try {
          hsAdminClient.run(args);
        } catch (Exception e) {
          fail("refreshLogRetentionSettings should have been successful");
        }
        return null;
      }
    });
    verify(loginUGI).doAs(any(PrivilegedExceptionAction.class));
    verify(alds).refreshLogRetentionSettings();

    reset(loginUGI);

    ugi.doAs(new PrivilegedAction<Void>() {
      @Override
      public Void run() {
        String[] args = new String[1];
        args[0] = "-refreshJobRetentionSettings";
        try {
          hsAdminClient.run(args);
        } catch (Exception e) {
          fail("refreshJobRetentionSettings should have been successful");
        }
        return null;
      }
    });
    verify(loginUGI).doAs(any(PrivilegedExceptionAction.class));
    verify(jobHistoryService).refreshJobRetentionSettings();
  }

  @After
  public void cleanUp() {
    if (hsAdminServer != null)

