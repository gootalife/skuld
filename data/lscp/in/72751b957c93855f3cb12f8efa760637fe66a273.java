hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/DFSClient.java
import org.apache.hadoop.hdfs.client.HdfsDataInputStream;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.apache.hadoop.hdfs.client.impl.DfsClientConf;
import org.apache.hadoop.hdfs.client.impl.LeaseRenewer;
import org.apache.hadoop.hdfs.net.Peer;
import org.apache.hadoop.hdfs.net.TcpPeerServer;
import org.apache.hadoop.hdfs.protocol.AclException;
  public void putFileBeingWritten(final long inodeId, final DFSOutputStream out) {
    synchronized(filesBeingWritten) {
      filesBeingWritten.put(inodeId, out);
  }

  public void removeFileBeingWritten(final long inodeId) {
    synchronized(filesBeingWritten) {
      filesBeingWritten.remove(inodeId);
      if (filesBeingWritten.isEmpty()) {
  }

  public boolean isFilesBeingWrittenEmpty() {
    synchronized(filesBeingWritten) {
      return filesBeingWritten.isEmpty();
    }
  }
  
  public boolean isClientRunning() {
    return clientRunning;
  }

  public boolean renewLease() throws IOException {
    if (clientRunning && !isFilesBeingWrittenEmpty()) {
      try {
        namenode.renewLease(clientName);
  }
  
  public void abort() {
    clientRunning = false;
    closeAllFilesBeingWritten(true);
    try {

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/client/impl/LeaseRenewer.java
++ b/hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/client/impl/LeaseRenewer.java
package org.apache.hadoop.hdfs.client.impl;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSOutputStream;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;
import com.google.common.annotations.VisibleForTesting;

@InterfaceAudience.Private
public class LeaseRenewer {
  static final Log LOG = LogFactory.getLog(LeaseRenewer.class);

  static final long LEASE_RENEWER_GRACE_DEFAULT = 60*1000L;
  static final long LEASE_RENEWER_SLEEP_DEFAULT = 1000L;

  public static LeaseRenewer getInstance(final String authority,
      final UserGroupInformation ugi, final DFSClient dfsc) throws IOException {
    final LeaseRenewer r = Factory.INSTANCE.get(authority, ugi);
    r.addClient(dfsc);
    return r;
  }

  private static class Factory {
    private static final Factory INSTANCE = new Factory();

    private static class Key {
      final String authority;
      final UserGroupInformation ugi;

      private Key(final String authority, final UserGroupInformation ugi) {
        if (authority == null) {
          throw new HadoopIllegalArgumentException("authority == null");
        } else if (ugi == null) {
          throw new HadoopIllegalArgumentException("ugi == null");
        }

        this.authority = authority;
        this.ugi = ugi;
      }

      @Override
      public int hashCode() {
        return authority.hashCode() ^ ugi.hashCode();
      }

      @Override
      public boolean equals(Object obj) {
        if (obj == this) {
          return true;
        }
        if (obj != null && obj instanceof Key) {
          final Key that = (Key)obj;
          return this.authority.equals(that.authority)
                 && this.ugi.equals(that.ugi);
        }
        return false;
      }

      @Override
      public String toString() {
        return ugi.getShortUserName() + "@" + authority;
      }
    }

    private final Map<Key, LeaseRenewer> renewers = new HashMap<Key, LeaseRenewer>();

    private synchronized LeaseRenewer get(final String authority,
        final UserGroupInformation ugi) {
      final Key k = new Key(authority, ugi);
      LeaseRenewer r = renewers.get(k);
      if (r == null) {
        r = new LeaseRenewer(k);
        renewers.put(k, r);
      }
      return r;
    }

    private synchronized void remove(final LeaseRenewer r) {
      final LeaseRenewer stored = renewers.get(r.factorykey);
      if (r == stored) {
        if (!r.clientsRunning()) {
          renewers.remove(r.factorykey);
        }
      }
    }
  }

  private long emptyTime = Long.MAX_VALUE;
  private long renewal = HdfsConstants.LEASE_SOFTLIMIT_PERIOD/2;

  private Daemon daemon = null;
  private int currentId = 0;

  private long gracePeriod;
  private long sleepPeriod;

  private final Factory.Key factorykey;

  private final List<DFSClient> dfsclients = new ArrayList<DFSClient>();

  private final String instantiationTrace;

  private LeaseRenewer(Factory.Key factorykey) {
    this.factorykey = factorykey;
    unsyncSetGraceSleepPeriod(LEASE_RENEWER_GRACE_DEFAULT);

    if (LOG.isTraceEnabled()) {
      instantiationTrace = StringUtils.stringifyException(
        new Throwable("TRACE"));
    } else {
      instantiationTrace = null;
    }
  }

  private synchronized long getRenewalTime() {
    return renewal;
  }

  private synchronized void addClient(final DFSClient dfsc) {
    for(DFSClient c : dfsclients) {
      if (c == dfsc) {
        return;
      }
    }
    dfsclients.add(dfsc);

    final int hdfsTimeout = dfsc.getConf().getHdfsTimeout();
    if (hdfsTimeout > 0) {
      final long half = hdfsTimeout/2;
      if (half < renewal) {
        this.renewal = half;
      }
    }
  }

  private synchronized boolean clientsRunning() {
    for(Iterator<DFSClient> i = dfsclients.iterator(); i.hasNext(); ) {
      if (!i.next().isClientRunning()) {
        i.remove();
      }
    }
    return !dfsclients.isEmpty();
  }

  private synchronized long getSleepPeriod() {
    return sleepPeriod;
  }

  synchronized void setGraceSleepPeriod(final long gracePeriod) {
    unsyncSetGraceSleepPeriod(gracePeriod);
  }

  private void unsyncSetGraceSleepPeriod(final long gracePeriod) {
    if (gracePeriod < 100L) {
      throw new HadoopIllegalArgumentException(gracePeriod
          + " = gracePeriod < 100ms is too small.");
    }
    this.gracePeriod = gracePeriod;
    final long half = gracePeriod/2;
    this.sleepPeriod = half < LEASE_RENEWER_SLEEP_DEFAULT?
        half: LEASE_RENEWER_SLEEP_DEFAULT;
  }

  synchronized boolean isRunning() {
    return daemon != null && daemon.isAlive();
  }

  public boolean isEmpty() {
    return dfsclients.isEmpty();
  }

  synchronized String getDaemonName() {
    return daemon.getName();
  }

  private synchronized boolean isRenewerExpired() {
    return emptyTime != Long.MAX_VALUE
        && Time.monotonicNow() - emptyTime > gracePeriod;
  }

  public synchronized void put(final long inodeId, final DFSOutputStream out,
      final DFSClient dfsc) {
    if (dfsc.isClientRunning()) {
      if (!isRunning() || isRenewerExpired()) {
        final int id = ++currentId;
        daemon = new Daemon(new Runnable() {
          @Override
          public void run() {
            try {
              if (LOG.isDebugEnabled()) {
                LOG.debug("Lease renewer daemon for " + clientsString()
                    + " with renew id " + id + " started");
              }
              LeaseRenewer.this.run(id);
            } catch(InterruptedException e) {
              if (LOG.isDebugEnabled()) {
                LOG.debug(LeaseRenewer.this.getClass().getSimpleName()
                    + " is interrupted.", e);
              }
            } finally {
              synchronized(LeaseRenewer.this) {
                Factory.INSTANCE.remove(LeaseRenewer.this);
              }
              if (LOG.isDebugEnabled()) {
                LOG.debug("Lease renewer daemon for " + clientsString()
                    + " with renew id " + id + " exited");
              }
            }
          }

          @Override
          public String toString() {
            return String.valueOf(LeaseRenewer.this);
          }
        });
        daemon.start();
      }
      dfsc.putFileBeingWritten(inodeId, out);
      emptyTime = Long.MAX_VALUE;
    }
  }

  @VisibleForTesting
  synchronized void setEmptyTime(long time) {
    emptyTime = time;
  }

  public void closeFile(final long inodeId, final DFSClient dfsc) {
    dfsc.removeFileBeingWritten(inodeId);

    synchronized(this) {
      if (dfsc.isFilesBeingWrittenEmpty()) {
        dfsclients.remove(dfsc);
      }
      if (emptyTime == Long.MAX_VALUE) {
        for(DFSClient c : dfsclients) {
          if (!c.isFilesBeingWrittenEmpty()) {
            return;
          }
        }
        emptyTime = Time.monotonicNow();
      }
    }
  }

  public synchronized void closeClient(final DFSClient dfsc) {
    dfsclients.remove(dfsc);
    if (dfsclients.isEmpty()) {
      if (!isRunning() || isRenewerExpired()) {
        Factory.INSTANCE.remove(LeaseRenewer.this);
        return;
      }
      if (emptyTime == Long.MAX_VALUE) {
        emptyTime = Time.monotonicNow();
      }
    }

    if (renewal == dfsc.getConf().getHdfsTimeout()/2) {
      long min = HdfsConstants.LEASE_SOFTLIMIT_PERIOD;
      for(DFSClient c : dfsclients) {
        final int timeout = c.getConf().getHdfsTimeout();
        if (timeout > 0 && timeout < min) {
          min = timeout;
        }
      }
      renewal = min/2;
    }
  }

  public void interruptAndJoin() throws InterruptedException {
    Daemon daemonCopy = null;
    synchronized (this) {
      if (isRunning()) {
        daemon.interrupt();
        daemonCopy = daemon;
      }
    }

    if (daemonCopy != null) {
      if(LOG.isDebugEnabled()) {
        LOG.debug("Wait for lease checker to terminate");
      }
      daemonCopy.join();
    }
  }

  private void renew() throws IOException {
    final List<DFSClient> copies;
    synchronized(this) {
      copies = new ArrayList<DFSClient>(dfsclients);
    }
    Collections.sort(copies, new Comparator<DFSClient>() {
      @Override
      public int compare(final DFSClient left, final DFSClient right) {
        return left.getClientName().compareTo(right.getClientName());
      }
    });
    String previousName = "";
    for(int i = 0; i < copies.size(); i++) {
      final DFSClient c = copies.get(i);
      if (!c.getClientName().equals(previousName)) {
        if (!c.renewLease()) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Did not renew lease for client " +
                c);
          }
          continue;
        }
        previousName = c.getClientName();
        if (LOG.isDebugEnabled()) {
          LOG.debug("Lease renewed for client " + previousName);
        }
      }
    }
  }

  private void run(final int id) throws InterruptedException {
    for(long lastRenewed = Time.monotonicNow(); !Thread.interrupted();
        Thread.sleep(getSleepPeriod())) {
      final long elapsed = Time.monotonicNow() - lastRenewed;
      if (elapsed >= getRenewalTime()) {
        try {
          renew();
          if (LOG.isDebugEnabled()) {
            LOG.debug("Lease renewer daemon for " + clientsString()
                + " with renew id " + id + " executed");
          }
          lastRenewed = Time.monotonicNow();
        } catch (SocketTimeoutException ie) {
          LOG.warn("Failed to renew lease for " + clientsString() + " for "
              + (elapsed/1000) + " seconds.  Aborting ...", ie);
          synchronized (this) {
            while (!dfsclients.isEmpty()) {
              dfsclients.get(0).abort();
            }
          }
          break;
        } catch (IOException ie) {
          LOG.warn("Failed to renew lease for " + clientsString() + " for "
              + (elapsed/1000) + " seconds.  Will retry shortly ...", ie);
        }
      }

      synchronized(this) {
        if (id != currentId || isRenewerExpired()) {
          if (LOG.isDebugEnabled()) {
            if (id != currentId) {
              LOG.debug("Lease renewer daemon for " + clientsString()
                  + " with renew id " + id + " is not current");
            } else {
               LOG.debug("Lease renewer daemon for " + clientsString()
                  + " with renew id " + id + " expired");
            }
          }
          return;
        }

        if (!clientsRunning() && emptyTime == Long.MAX_VALUE) {
          emptyTime = Time.monotonicNow();
        }
      }
    }
  }

  @Override
  public String toString() {
    String s = getClass().getSimpleName() + ":" + factorykey;
    if (LOG.isTraceEnabled()) {
      return s + ", clients=" +  clientsString()
        + ", created at " + instantiationTrace;
    }
    return s;
  }

  private synchronized String clientsString() {
    if (dfsclients.isEmpty()) {
      return "[]";
    } else {
      final StringBuilder b = new StringBuilder("[").append(
          dfsclients.get(0).getClientName());
      for(int i = 1; i < dfsclients.size(); i++) {
        b.append(", ").append(dfsclients.get(i).getClientName());
      }
      return b.append("]").toString();
    }
  }
}

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestDistributedFileSystem.java
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.SocketTimeoutException;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.MiniDFSCluster.DataNodeProperties;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.client.impl.LeaseRenewer;
import org.apache.hadoop.hdfs.net.Peer;
import org.apache.hadoop.hdfs.server.datanode.DataNodeFaultInjector;
import org.apache.hadoop.hdfs.server.namenode.ha.HATestUtil;

      {
        final DistributedFileSystem dfs = cluster.getFileSystem();
        Method setMethod = dfs.dfs.getLeaseRenewer().getClass()
            .getDeclaredMethod("setGraceSleepPeriod", long.class);
        setMethod.setAccessible(true);
        setMethod.invoke(dfs.dfs.getLeaseRenewer(), grace);
        Method checkMethod = dfs.dfs.getLeaseRenewer().getClass()
            .getDeclaredMethod("isRunning");
        checkMethod.setAccessible(true);
        assertFalse((boolean) checkMethod.invoke(dfs.dfs.getLeaseRenewer()));
  
        {
          final FSDataOutputStream out = dfs.create(filepaths[0]);
          assertTrue((boolean)checkMethod.invoke(dfs.dfs.getLeaseRenewer()));
          out.writeLong(millis);
          assertTrue((boolean)checkMethod.invoke(dfs.dfs.getLeaseRenewer()));
          out.close();
          Thread.sleep(grace/4*3);
          assertTrue((boolean)checkMethod.invoke(dfs.dfs.getLeaseRenewer()));
          for(int i = 0; i < 3; i++) {
            if ((boolean)checkMethod.invoke(dfs.dfs.getLeaseRenewer())) {
              Thread.sleep(grace/2);
            }
          }
          assertFalse((boolean)checkMethod.invoke(dfs.dfs.getLeaseRenewer()));
        }

        {
          final FSDataOutputStream out1 = dfs.create(filepaths[1]);
          assertTrue((boolean)checkMethod.invoke(dfs.dfs.getLeaseRenewer()));
          final FSDataOutputStream out2 = dfs.create(filepaths[2]);
          assertTrue((boolean)checkMethod.invoke(dfs.dfs.getLeaseRenewer()));

          out1.writeLong(millis);
          assertTrue((boolean)checkMethod.invoke(dfs.dfs.getLeaseRenewer()));
          out1.close();
          assertTrue((boolean)checkMethod.invoke(dfs.dfs.getLeaseRenewer()));

          out2.writeLong(millis);
          assertTrue((boolean)checkMethod.invoke(dfs.dfs.getLeaseRenewer()));
          out2.close();
          Thread.sleep(grace/4*3);
          assertTrue((boolean)checkMethod.invoke(dfs.dfs.getLeaseRenewer()));
        }

        {
          final FSDataOutputStream out3 = dfs.create(filepaths[3]);
          assertTrue((boolean)checkMethod.invoke(dfs.dfs.getLeaseRenewer()));
          Thread.sleep(grace/4*3);
          assertTrue((boolean)checkMethod.invoke(dfs.dfs.getLeaseRenewer()));
          out3.writeLong(millis);
          assertTrue((boolean)checkMethod.invoke(dfs.dfs.getLeaseRenewer()));
          out3.close();
          assertTrue((boolean)checkMethod.invoke(dfs.dfs.getLeaseRenewer()));
          Thread.sleep(grace/4*3);
          assertTrue((boolean)checkMethod.invoke(dfs.dfs.getLeaseRenewer()));
          for(int i = 0; i < 3; i++) {
            if ((boolean)checkMethod.invoke(dfs.dfs.getLeaseRenewer())) {
              Thread.sleep(grace/2);
            }
          }
          assertFalse((boolean)checkMethod.invoke(dfs.dfs.getLeaseRenewer()));
        }

        dfs.close();

      {
        final DistributedFileSystem dfs = cluster.getFileSystem();
        Method checkMethod = dfs.dfs.getLeaseRenewer().getClass()
            .getDeclaredMethod("isRunning");
        checkMethod.setAccessible(true);
        assertFalse((boolean)checkMethod.invoke(dfs.dfs.getLeaseRenewer()));

        FSDataInputStream in = dfs.open(filepaths[0]);
        assertFalse((boolean)checkMethod.invoke(dfs.dfs.getLeaseRenewer()));
        assertEquals(millis, in.readLong());
        assertFalse((boolean)checkMethod.invoke(dfs.dfs.getLeaseRenewer()));
        in.close();
        assertFalse((boolean)checkMethod.invoke(dfs.dfs.getLeaseRenewer()));
        dfs.close();
      }
      

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestLease.java
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.client.impl.LeaseRenewer;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/client/impl/TestLeaseRenewer.java
++ b/hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/client/impl/TestLeaseRenewer.java
package org.apache.hadoop.hdfs.client.impl;

import static org.junit.Assert.assertSame;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSOutputStream;
import org.apache.hadoop.hdfs.client.impl.DfsClientConf;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.Time;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.common.base.Supplier;

public class TestLeaseRenewer {
  private final String FAKE_AUTHORITY="hdfs://nn1/";
  private final UserGroupInformation FAKE_UGI_A =
    UserGroupInformation.createUserForTesting(
      "myuser", new String[]{"group1"});
  private final UserGroupInformation FAKE_UGI_B =
    UserGroupInformation.createUserForTesting(
      "myuser", new String[]{"group1"});

  private DFSClient MOCK_DFSCLIENT;
  private LeaseRenewer renewer;

  private static final long FAST_GRACE_PERIOD = 100L;

  @Before
  public void setupMocksAndRenewer() throws IOException {
    MOCK_DFSCLIENT = createMockClient();

    renewer = LeaseRenewer.getInstance(
        FAKE_AUTHORITY, FAKE_UGI_A, MOCK_DFSCLIENT);
    renewer.setGraceSleepPeriod(FAST_GRACE_PERIOD);
}

  private DFSClient createMockClient() {
    final DfsClientConf mockConf = Mockito.mock(DfsClientConf.class);
    Mockito.doReturn((int)FAST_GRACE_PERIOD).when(mockConf).getHdfsTimeout();

    DFSClient mock = Mockito.mock(DFSClient.class);
    Mockito.doReturn(true).when(mock).isClientRunning();
    Mockito.doReturn(mockConf).when(mock).getConf();
    Mockito.doReturn("myclient").when(mock).getClientName();
    return mock;
  }

  @Test
  public void testInstanceSharing() throws IOException {
    LeaseRenewer lr = LeaseRenewer.getInstance(
        FAKE_AUTHORITY, FAKE_UGI_A, MOCK_DFSCLIENT);
    LeaseRenewer lr2 = LeaseRenewer.getInstance(
        FAKE_AUTHORITY, FAKE_UGI_A, MOCK_DFSCLIENT);
    Assert.assertSame(lr, lr2);

    LeaseRenewer lr3 = LeaseRenewer.getInstance(
        FAKE_AUTHORITY, FAKE_UGI_B, MOCK_DFSCLIENT);
    Assert.assertNotSame(lr, lr3);

    LeaseRenewer lr4 = LeaseRenewer.getInstance(
        "someOtherAuthority", FAKE_UGI_B, MOCK_DFSCLIENT);
    Assert.assertNotSame(lr, lr4);
    Assert.assertNotSame(lr3, lr4);
  }

  @Test
  public void testRenewal() throws Exception {
    final AtomicInteger leaseRenewalCount = new AtomicInteger();
    Mockito.doAnswer(new Answer<Boolean>() {
      @Override
      public Boolean answer(InvocationOnMock invocation) throws Throwable {
        leaseRenewalCount.incrementAndGet();
        return true;
      }
    }).when(MOCK_DFSCLIENT).renewLease();


    DFSOutputStream mockStream = Mockito.mock(DFSOutputStream.class);
    long fileId = 123L;
    renewer.put(fileId, mockStream, MOCK_DFSCLIENT);

    long failTime = Time.monotonicNow() + 5000;
    while (Time.monotonicNow() < failTime &&
        leaseRenewalCount.get() == 0) {
      Thread.sleep(50);
    }
    if (leaseRenewalCount.get() == 0) {
      Assert.fail("Did not renew lease at all!");
    }

    renewer.closeFile(fileId, MOCK_DFSCLIENT);
  }

  @Test
  public void testManyDfsClientsWhereSomeNotOpen() throws Exception {
    final DFSClient mockClient1 = createMockClient();
    Mockito.doReturn(false).when(mockClient1).renewLease();
    assertSame(renewer, LeaseRenewer.getInstance(
        FAKE_AUTHORITY, FAKE_UGI_A, mockClient1));

    DFSOutputStream mockStream1 = Mockito.mock(DFSOutputStream.class);
    long fileId = 456L;
    renewer.put(fileId, mockStream1, mockClient1);

    final DFSClient mockClient2 = createMockClient();
    Mockito.doReturn(true).when(mockClient2).renewLease();
    assertSame(renewer, LeaseRenewer.getInstance(
        FAKE_AUTHORITY, FAKE_UGI_A, mockClient2));

    DFSOutputStream mockStream2 = Mockito.mock(DFSOutputStream.class);
    renewer.put(fileId, mockStream2, mockClient2);


    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        try {
          Mockito.verify(mockClient1, Mockito.atLeastOnce()).renewLease();
          Mockito.verify(mockClient2, Mockito.atLeastOnce()).renewLease();
          return true;
        } catch (AssertionError err) {
          LeaseRenewer.LOG.warn("Not yet satisfied", err);
          return false;
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    }, 100, 10000);

    renewer.closeFile(fileId, mockClient1);
    renewer.closeFile(fileId, mockClient2);
  }

  @Test
  public void testThreadName() throws Exception {
    DFSOutputStream mockStream = Mockito.mock(DFSOutputStream.class);
    long fileId = 789L;
    Assert.assertFalse("Renewer not initially running",
        renewer.isRunning());

    renewer.put(fileId, mockStream, MOCK_DFSCLIENT);

    Assert.assertTrue("Renewer should have started running",
        renewer.isRunning());

    String threadName = renewer.getDaemonName();
    Assert.assertEquals("LeaseRenewer:myuser@hdfs://nn1/", threadName);

    renewer.closeFile(fileId, MOCK_DFSCLIENT);
    renewer.setEmptyTime(Time.monotonicNow());

    long failTime = Time.monotonicNow() + 5000;
    while (renewer.isRunning() && Time.monotonicNow() < failTime) {
      Thread.sleep(50);
    }
    Assert.assertFalse(renewer.isRunning());
  }

}

