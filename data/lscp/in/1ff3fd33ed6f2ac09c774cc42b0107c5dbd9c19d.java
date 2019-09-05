hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/security/DelegationTokenRenewer.java
import java.nio.ByteBuffer;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
  @VisibleForTesting
  protected static class DelegationTokenToRenew {
    public final Token<?> token;
    public final Collection<ApplicationId> referringAppIds;
    public final Configuration conf;
    public long expirationDate;
    public RenewalTimerTask timerTask;
    public volatile boolean shouldCancelAtEnd;
    public long maxDate;
    public String user;

    public DelegationTokenToRenew(Collection<ApplicationId> applicationIds,
        Token<?> token,
        Configuration conf, long expirationDate, boolean shouldCancelAtEnd,
        String user) {
      this.token = token;
          throw new YarnRuntimeException(e);
        }
      }
      this.referringAppIds = Collections.synchronizedSet(
          new HashSet<ApplicationId>(applicationIds));
      this.conf = conf;
      this.expirationDate = expirationDate;
      this.timerTask = null;
      this.shouldCancelAtEnd = shouldCancelAtEnd;
    }
    
    public void setTimerTask(RenewalTimerTask tTask) {
      timerTask = tTask;
    }

    @VisibleForTesting
    public void cancelTimer() {
      if (timerTask != null) {
        timerTask.cancel();
      }
    }

    @VisibleForTesting
    public boolean isTimerCancelled() {
      return (timerTask != null) && timerTask.cancelled.get();
    }

    @Override
    public String toString() {
      return token + ";exp=" + expirationDate + "; apps=" + referringAppIds;
    }
    
    @Override
        }

        DelegationTokenToRenew dttr = allTokens.get(token);
        if (dttr == null) {
          dttr = new DelegationTokenToRenew(Arrays.asList(applicationId), token,
              getConfig(), now, shouldCancelAtEnd, evt.getUser());
          try {
            renewToken(dttr);
          } catch (IOException ioe) {
            throw new IOException("Failed to renew token: " + dttr.token, ioe);
          }
        }
        tokenList.add(dttr);
      }
    }

      for (DelegationTokenToRenew dtr : tokenList) {
        DelegationTokenToRenew currentDtr =
            allTokens.putIfAbsent(dtr.token, dtr);
        if (currentDtr != null) {
          currentDtr.referringAppIds.add(applicationId);
          appTokens.get(applicationId).add(currentDtr);
        } else {
          appTokens.get(applicationId).add(dtr);
          setTimerForTokenRenewal(dtr);
        }
      }
    }

    if (!hasHdfsToken) {
      requestNewHdfsDelegationToken(Arrays.asList(applicationId), evt.getUser(),
        shouldCancelAtEnd);
    }
  }
      try {
        requestNewHdfsDelegationTokenIfNeeded(dttr);
        if (!dttr.isTimerCancelled()) {
          renewToken(dttr);
          setTimerForTokenRenewal(dttr);// set the next one
        } else {
    long expiresIn = token.expirationDate - System.currentTimeMillis();
    long renewIn = token.expirationDate - expiresIn/10; // little bit before the expiration
    RenewalTimerTask tTask = new RenewalTimerTask(token);
    token.setTimerTask(tTask); // keep reference to the timer

    renewalTimer.schedule(token.timerTask, new Date(renewIn));
    LOG.info("Renew " + token + " in " + expiresIn + " ms, appId = "
        + token.referringAppIds);
  }

      throw new IOException(e);
    }
    LOG.info("Renewed delegation-token= [" + dttr + "], for "
        + dttr.referringAppIds);
  }

        && dttr.maxDate - dttr.expirationDate < credentialsValidTimeRemaining
        && dttr.token.getKind().equals(new Text("HDFS_DELEGATION_TOKEN"))) {

      final Collection<ApplicationId> applicationIds;
      synchronized (dttr.referringAppIds) {
        applicationIds = new HashSet<>(dttr.referringAppIds);
        dttr.referringAppIds.clear();
      }
      for (ApplicationId appId : applicationIds) {
        Set<DelegationTokenToRenew> tokenSet = appTokens.get(appId);
        if (tokenSet == null || tokenSet.isEmpty()) {
          continue;
        }
        Iterator<DelegationTokenToRenew> iter = tokenSet.iterator();
        synchronized (tokenSet) {
          while (iter.hasNext()) {
            DelegationTokenToRenew t = iter.next();
            if (t.token.getKind().equals(new Text("HDFS_DELEGATION_TOKEN"))) {
              iter.remove();
              t.cancelTimer();
              LOG.info("Removed expiring token " + t);
            }
          }
        }
      }
      LOG.info("Token= (" + dttr + ") is expiring, request new token.");
      requestNewHdfsDelegationToken(applicationIds, dttr.user,
          dttr.shouldCancelAtEnd);
    }
  }

  private void requestNewHdfsDelegationToken(
      Collection<ApplicationId> referringAppIds,
      String user, boolean shouldCancelAtEnd) throws IOException,
      InterruptedException {
    if (!hasProxyUserPrivileges) {
    Token<?>[] newTokens = obtainSystemTokensForUser(user, credentials);

    LOG.info("Received new tokens for " + referringAppIds + ". Received "
        + newTokens.length + " tokens.");
    if (newTokens.length > 0) {
      for (Token<?> token : newTokens) {
        if (token.isManaged()) {
          DelegationTokenToRenew tokenToRenew =
              new DelegationTokenToRenew(referringAppIds, token, getConfig(),
                Time.now(), shouldCancelAtEnd, user);
          renewToken(tokenToRenew);
          setTimerForTokenRenewal(tokenToRenew);
          for (ApplicationId applicationId : referringAppIds) {
            appTokens.get(applicationId).add(tokenToRenew);
          }
          LOG.info("Received new token " + token);
        }
      }
    DataOutputBuffer dob = new DataOutputBuffer();
    credentials.writeTokenStorageToStream(dob);
    ByteBuffer byteBuffer = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
    for (ApplicationId applicationId : referringAppIds) {
      rmContext.getSystemCredentialsForApps().put(applicationId, byteBuffer);
    }
  }

  @VisibleForTesting
  protected Token<?>[] obtainSystemTokensForUser(String user,
  private void removeFailedDelegationToken(DelegationTokenToRenew t) {
    Collection<ApplicationId> applicationIds = t.referringAppIds;
    synchronized (applicationIds) {
      LOG.error("removing failed delegation token for appid=" + applicationIds
          + ";t=" + t.token.getService());
      for (ApplicationId applicationId : applicationIds) {
        appTokens.get(applicationId).remove(t);
      }
    }
    allTokens.remove(t.token);

    t.cancelTimer();
  }

                + "; token=" + dttr.token.getService());
          }

          synchronized(dttr.referringAppIds) {
            dttr.referringAppIds.remove(applicationId);
            if (!dttr.referringAppIds.isEmpty()) {
              continue;
            }
          }
          dttr.cancelTimer();

          cancelToken(dttr);

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/security/TestDelegationTokenRenewer.java
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.TestUtils;
import org.apache.hadoop.yarn.server.resourcemanager.security.DelegationTokenRenewer.DelegationTokenToRenew;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.junit.After;
import org.junit.Assert;
      counter = 0;
      lastRenewed = null;
      tokenToRenewIn2Sec = null;
      cancelled = false;
    }

    @Override
    delegationTokenRenewer.obtainSystemTokensForUser(user, credentials);
    Assert.assertEquals(oldCounter, MyFS.getInstanceCounter());
  }
  
  @Test (timeout = 30000)
  public void testCancelWithMultipleAppSubmissions() throws Exception{
    MockRM rm = new TestSecurityMockRM(conf, null);
    rm.start();
    final MockNM nm1 =
        new MockNM("127.0.0.1:1234", 15120, rm.getResourceTrackerService());
    nm1.registerNode();


    Text userText1 = new Text("user");
    DelegationTokenIdentifier dtId1 =
        new DelegationTokenIdentifier(userText1, new Text("renewer1"),
          userText1);
    final Token<DelegationTokenIdentifier> token1 =
        new Token<DelegationTokenIdentifier>(dtId1.getBytes(),
          "password1".getBytes(), dtId1.getKind(), new Text("service1"));

    Credentials credentials = new Credentials();
    credentials.addToken(token1.getService(), token1);

    DelegationTokenRenewer renewer =
        rm.getRMContext().getDelegationTokenRenewer();
    Assert.assertTrue(renewer.getAllTokens().isEmpty());
    Assert.assertFalse(Renewer.cancelled);

    RMApp app1 =
        rm.submitApp(200, "name", "user", null, false, null, 2, credentials,
          null, true, false, false, null, 0, null, true);
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm, nm1);
    rm.waitForState(app1.getApplicationId(), RMAppState.RUNNING);

    DelegationTokenToRenew dttr = renewer.getAllTokens().get(token1);
    Assert.assertNotNull(dttr);
    Assert.assertTrue(dttr.referringAppIds.contains(app1.getApplicationId()));
    RMApp app2 =
        rm.submitApp(200, "name", "user", null, false, null, 2, credentials,
          null, true, false, false, null, 0, null, true);
    MockAM am2 = MockRM.launchAndRegisterAM(app2, rm, nm1);
    rm.waitForState(app2.getApplicationId(), RMAppState.RUNNING);
    Assert.assertTrue(renewer.getAllTokens().containsKey(token1));
    Assert.assertTrue(dttr.referringAppIds.contains(app2.getApplicationId()));
    Assert.assertTrue(dttr.referringAppIds.contains(app2.getApplicationId()));
    Assert.assertFalse(Renewer.cancelled);

    MockRM.finishAMAndVerifyAppState(app2, rm, nm1, am2);
    Assert.assertTrue(renewer.getAllTokens().containsKey(token1));
    Assert.assertTrue(dttr.referringAppIds.contains(app1.getApplicationId()));
    Assert.assertFalse(dttr.referringAppIds.contains(app2.getApplicationId()));
    Assert.assertFalse(dttr.isTimerCancelled());
    Assert.assertFalse(Renewer.cancelled);

    RMApp app3 =
        rm.submitApp(200, "name", "user", null, false, null, 2, credentials,
          null, true, false, false, null, 0, null, true);
    MockAM am3 = MockRM.launchAndRegisterAM(app3, rm, nm1);
    rm.waitForState(app3.getApplicationId(), RMAppState.RUNNING);
    Assert.assertTrue(renewer.getAllTokens().containsKey(token1));
    Assert.assertTrue(dttr.referringAppIds.contains(app1.getApplicationId()));
    Assert.assertTrue(dttr.referringAppIds.contains(app3.getApplicationId()));
    Assert.assertFalse(dttr.isTimerCancelled());
    Assert.assertFalse(Renewer.cancelled);

    MockRM.finishAMAndVerifyAppState(app1, rm, nm1, am1);
    Assert.assertTrue(renewer.getAllTokens().containsKey(token1));
    Assert.assertFalse(dttr.referringAppIds.contains(app1.getApplicationId()));
    Assert.assertTrue(dttr.referringAppIds.contains(app3.getApplicationId()));
    Assert.assertFalse(dttr.isTimerCancelled());
    Assert.assertFalse(Renewer.cancelled);

    MockRM.finishAMAndVerifyAppState(app3, rm, nm1, am3);
    Assert.assertFalse(renewer.getAllTokens().containsKey(token1));
    Assert.assertTrue(dttr.referringAppIds.isEmpty());
    Assert.assertTrue(dttr.isTimerCancelled());
    Assert.assertTrue(Renewer.cancelled);
  }
}

