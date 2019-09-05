hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/security/token/delegation/ZKDelegationTokenSecretManager.java
import org.apache.curator.framework.recipes.shared.SharedCount;
import org.apache.curator.framework.recipes.shared.VersionedValue;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.utils.EnsurePath;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
      } catch (Exception e) {
        throw new IOException("Could not start Curator Framework", e);
      }
    } else {
      CuratorFramework nullNsFw = zkClient.usingNamespace(null);
      EnsurePath ensureNs =
        nullNsFw.newNamespaceAwareEnsurePath("/" + zkClient.getNamespace());
      try {
        ensureNs.ensure(nullNsFw.getZookeeperClient());
      } catch (Exception e) {
        throw new IOException("Could not create namespace", e);
      }
    }
    listenerThreadPool = Executors.newSingleThreadExecutor();
    try {

hadoop-common-project/hadoop-common/src/test/java/org/apache/hadoop/security/token/delegation/TestZKDelegationTokenSecretManager.java
package org.apache.hadoop.security.token.delegation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.ACLProvider;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenIdentifier;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenManager;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.server.auth.DigestAuthenticationProvider;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
    tm1.destroy();
  }

  @Test
  public void testACLs() throws Exception {
    DelegationTokenManager tm1;
    String connectString = zkServer.getConnectString();
    Configuration conf = getSecretConf(connectString);
    RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
    String userPass = "myuser:mypass";
    final ACL digestACL = new ACL(ZooDefs.Perms.ALL, new Id("digest",
      DigestAuthenticationProvider.generateDigest(userPass)));
    ACLProvider digestAclProvider = new ACLProvider() {
      @Override
      public List<ACL> getAclForPath(String path) { return getDefaultAcl(); }

      @Override
      public List<ACL> getDefaultAcl() {
        List<ACL> ret = new ArrayList<ACL>();
        ret.add(digestACL);
        return ret;
      }
    };

    CuratorFramework curatorFramework =
      CuratorFrameworkFactory.builder()
        .connectString(connectString)
        .retryPolicy(retryPolicy)
        .aclProvider(digestAclProvider)
        .authorization("digest", userPass.getBytes("UTF-8"))
        .build();
    curatorFramework.start();
    ZKDelegationTokenSecretManager.setCurator(curatorFramework);
    tm1 = new DelegationTokenManager(conf, new Text("bla"));
    tm1.init();

    String workingPath = conf.get(ZKDelegationTokenSecretManager.ZK_DTSM_ZNODE_WORKING_PATH);
    verifyACL(curatorFramework, "/" + workingPath, digestACL);

    tm1.destroy();
    ZKDelegationTokenSecretManager.setCurator(null);
    curatorFramework.close();
  }

  private void verifyACL(CuratorFramework curatorFramework,
      String path, ACL expectedACL) throws Exception {
    List<ACL> acls = curatorFramework.getACL().forPath(path);
    Assert.assertEquals(1, acls.size());
    Assert.assertEquals(expectedACL, acls.get(0));
  }


