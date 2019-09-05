hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/java/org/apache/hadoop/mapreduce/MRJobConfig.java

  public static final String JOB_NAMENODES = "mapreduce.job.hdfs-servers";

  public static final String JOB_NAMENODES_TOKEN_RENEWAL_EXCLUDE = "mapreduce.job.hdfs-servers.token-renewal.exclude";

  public static final String JOB_JOBTRACKER_ID = "mapreduce.job.kerberos.jtprinicipal";

  public static final String JOB_CANCEL_DELEGATION_TOKEN = "mapreduce.job.complete.cancel.delegation.tokens";

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/java/org/apache/hadoop/mapreduce/security/TokenCache.java
    }
  }

  static boolean isTokenRenewalExcluded(FileSystem fs, Configuration conf) {
    String [] nns =
        conf.getStrings(MRJobConfig.JOB_NAMENODES_TOKEN_RENEWAL_EXCLUDE);
    if (nns != null) {
      String host = fs.getUri().getHost();
      for(int i=0; i< nns.length; i++) {
        if (nns[i].equals(host)) {
          return true;
        }
      }
    }
    return false;
  }

  static void obtainTokensForNamenodesInternal(FileSystem fs, 
      Credentials credentials, Configuration conf) throws IOException {
    String delegTokenRenewer = "";
    if (!isTokenRenewalExcluded(fs, conf)) {
      delegTokenRenewer = Master.getMasterPrincipal(conf);
      if (delegTokenRenewer == null || delegTokenRenewer.length() == 0) {
        throw new IOException(
            "Can't get Master Kerberos principal for use as renewer");
      }
    }

    mergeBinaryTokens(credentials, conf);

    final Token<?> tokens[] = fs.addDelegationTokens(delegTokenRenewer,

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/security/DelegationTokenRenewer.java
  
  private static final Log LOG = 
      LogFactory.getLog(DelegationTokenRenewer.class);
  @VisibleForTesting
  public static final Text HDFS_DELEGATION_KIND =
      new Text("HDFS_DELEGATION_TOKEN");
  public static final String SCHEME = "hdfs";

        String user) {
      this.token = token;
      this.user = user;
      if (token.getKind().equals(HDFS_DELEGATION_KIND)) {
        try {
          AbstractDelegationTokenIdentifier identifier =
              (AbstractDelegationTokenIdentifier) token.decodeIdentifier();
    boolean hasHdfsToken = false;
    for (Token<?> token : tokens) {
      if (token.isManaged()) {
        if (token.getKind().equals(HDFS_DELEGATION_KIND)) {
          LOG.info(applicationId + " found existing hdfs token " + token);
          hasHdfsToken = true;
        }
        if (skipTokenRenewal(token)) {
          continue;
        }

        DelegationTokenToRenew dttr = allTokens.get(token);
        if (dttr == null) {
    }
  }

  private boolean skipTokenRenewal(Token<?> token)
      throws IOException {
    @SuppressWarnings("unchecked")
    Text renewer = ((Token<AbstractDelegationTokenIdentifier>)token).
        decodeIdentifier().getRenewer();
    return (renewer != null && renewer.toString().equals(""));
  }


    if (hasProxyUserPrivileges
        && dttr.maxDate - dttr.expirationDate < credentialsValidTimeRemaining
        && dttr.token.getKind().equals(HDFS_DELEGATION_KIND)) {

      final Collection<ApplicationId> applicationIds;
      synchronized (dttr.referringAppIds) {
        synchronized (tokenSet) {
          while (iter.hasNext()) {
            DelegationTokenToRenew t = iter.next();
            if (t.token.getKind().equals(HDFS_DELEGATION_KIND)) {
              iter.remove();
              allTokens.remove(t.token);
              t.cancelTimer();

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/security/TestDelegationTokenRenewer.java
public class TestDelegationTokenRenewer {
  private static final Log LOG = 
      LogFactory.getLog(TestDelegationTokenRenewer.class);
  private static final Text KIND =
      DelegationTokenRenewer.HDFS_DELEGATION_KIND;
  
  private static BlockingQueue<Event> eventQueue;
  private static volatile AtomicInteger counter;
    fail("App submission with a cancelled token should have failed");
  }

  @Test(timeout=60000)
  public void testAppTokenWithNonRenewer() throws Exception {
    MyFS dfs = (MyFS)FileSystem.get(conf);
    LOG.info("dfs="+(Object)dfs.hashCode() + ";conf="+conf.hashCode());

    MyToken token = dfs.getDelegationToken("");
    token.cancelToken();

    Credentials ts = new Credentials();
    ts.addToken(token.getKind(), token);
    
    ApplicationId appId =  BuilderUtils.newApplicationId(0, 0);
    delegationTokenRenewer.addApplicationSync(appId, ts, true, "user");
  }

      throws IOException, InterruptedException, BrokenBarrierException {
    final Credentials credsx = new Credentials();
    final Token<DelegationTokenIdentifier> tokenx = mock(Token.class);
    when(tokenx.getKind()).thenReturn(KIND);
    DelegationTokenIdentifier dtId1 = 
        new DelegationTokenIdentifier(new Text("user1"), new Text("renewer"),
          new Text("user1"));
    final Credentials creds1 = new Credentials();                              
    final Token<DelegationTokenIdentifier> token1 = mock(Token.class);    
    when(token1.getKind()).thenReturn(KIND);
    DelegationTokenIdentifier dtId1 = 
        new DelegationTokenIdentifier(new Text("user1"), new Text("renewer"),
          new Text("user1"));
    final Credentials creds2 = new Credentials();                              
    final Token<DelegationTokenIdentifier> token2 = mock(Token.class);           
    when(token2.getKind()).thenReturn(KIND);
    when(token2.decodeIdentifier()).thenReturn(dtId1);
    creds2.addToken(new Text("token"), token2);                                
    doReturn(true).when(token2).isManaged();                                   

