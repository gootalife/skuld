hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/fs/AbstractFileSystem.java
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
        + " doesn't support deleteSnapshot");
  }

  public void setStoragePolicy(final Path path, final String policyName)
      throws IOException {
    throw new UnsupportedOperationException(getClass().getSimpleName()
        + " doesn't support setStoragePolicy");
  }

  public Collection<? extends BlockStoragePolicySpi> getAllStoragePolicies()
      throws IOException {
    throw new UnsupportedOperationException(getClass().getSimpleName()
        + " doesn't support getAllStoragePolicies");
  }

  @Override //Object
  public int hashCode() {
    return myUri.hashCode();

hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/fs/BlockStoragePolicySpi.java
++ b/hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/fs/BlockStoragePolicySpi.java

package org.apache.hadoop.fs;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

@InterfaceAudience.Public
@InterfaceStability.Stable
public interface BlockStoragePolicySpi {

  String getName();

  StorageType[] getStorageTypes();

  StorageType[] getCreationFallbacks();

  StorageType[] getReplicationFallbacks();

  boolean isCopyOnCreateFile();
}

hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/fs/FileContext.java
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.IdentityHashMap;
      }
    }.resolve(this, absF);
  }

  public void setStoragePolicy(final Path path, final String policyName)
      throws IOException {
    final Path absF = fixRelativePart(path);
    new FSLinkResolver<Void>() {
      @Override
      public Void next(final AbstractFileSystem fs, final Path p)
          throws IOException {
        fs.setStoragePolicy(path, policyName);
        return null;
      }
    }.resolve(this, absF);
  }

  public Collection<? extends BlockStoragePolicySpi> getAllStoragePolicies()
      throws IOException {
    return defaultFS.getAllStoragePolicies();
  }
}

hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/fs/FileSystem.java
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
        + " doesn't support removeXAttr");
  }

  public void setStoragePolicy(final Path src, final String policyName)
      throws IOException {
    throw new UnsupportedOperationException(getClass().getSimpleName()
        + " doesn't support setStoragePolicy");
  }

  public Collection<? extends BlockStoragePolicySpi> getAllStoragePolicies()
      throws IOException {
    throw new UnsupportedOperationException(getClass().getSimpleName()
        + " doesn't support getAllStoragePolicies");
  }

  private volatile static boolean FILE_SYSTEMS_LOADED = false;


hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/fs/FilterFileSystem.java
import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
  public void removeXAttr(Path path, String name) throws IOException {
    fs.removeXAttr(path, name);
  }

  @Override
  public void setStoragePolicy(Path src, String policyName)
      throws IOException {
    fs.setStoragePolicy(src, policyName);
  }

  @Override
  public Collection<? extends BlockStoragePolicySpi> getAllStoragePolicies()
      throws IOException {
    return fs.getAllStoragePolicies();
  }
}

hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/fs/FilterFs.java
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
      throws IOException {
    myFs.deleteSnapshot(path, snapshotName);
  }

  @Override
  public void setStoragePolicy(Path path, String policyName)
      throws IOException {
    myFs.setStoragePolicy(path, policyName);
  }

  @Override
  public Collection<? extends BlockStoragePolicySpi> getAllStoragePolicies()
      throws IOException {
    return myFs.getAllStoragePolicies();
  }
}

hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/fs/viewfs/ChRootedFs.java
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.AbstractFileSystem;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.BlockStoragePolicySpi;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
    myFs.deleteSnapshot(fullPath(snapshotDir), snapshotName);
  }

  @Override
  public void setStoragePolicy(Path path, String policyName)
    throws IOException {
    myFs.setStoragePolicy(fullPath(path), policyName);
  }

  @Override
  public Collection<? extends BlockStoragePolicySpi> getAllStoragePolicies()
      throws IOException {
    return myFs.getAllStoragePolicies();
  }

  @Override
  public void setVerifyChecksum(final boolean verifyChecksum)
      throws IOException, UnresolvedLinkException {

hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/fs/viewfs/ViewFs.java
    res.targetFileSystem.deleteSnapshot(res.remainingPath, snapshotName);
  }

  @Override
  public void setStoragePolicy(final Path path, final String policyName)
      throws IOException {
    InodeTree.ResolveResult<AbstractFileSystem> res =
        fsState.resolve(getUriPath(path), true);
    res.targetFileSystem.setStoragePolicy(res.remainingPath, policyName);
  }

      checkPathIsSlash(path);
      throw readOnlyMountTable("deleteSnapshot", path);
    }

    @Override
    public void setStoragePolicy(Path path, String policyName)
        throws IOException {
      throw readOnlyMountTable("setStoragePolicy", path);
    }
  }
}

hadoop-common-project/hadoop-common/src/test/java/org/apache/hadoop/fs/TestHarFileSystem.java
import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Collection;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
    public AclStatus getAclStatus(Path path) throws IOException;

    public void access(Path path, FsAction mode) throws IOException;

    public void setStoragePolicy(Path src, String policyName)
        throws IOException;

    public Collection<? extends BlockStoragePolicySpi> getAllStoragePolicies()
        throws IOException;
  }

  @Test

hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/protocol/BlockStoragePolicy.java

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.BlockStoragePolicySpi;
import org.apache.hadoop.fs.StorageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
@InterfaceAudience.Private
public class BlockStoragePolicy implements BlockStoragePolicySpi {
  public static final Logger LOG = LoggerFactory.getLogger(BlockStoragePolicy
      .class);

    return id;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public StorageType[] getStorageTypes() {
    return this.storageTypes;
  }

  @Override
  public StorageType[] getCreationFallbacks() {
    return this.creationFallbacks;
  }

  @Override
  public StorageType[] getReplicationFallbacks() {
    return this.replicationFallbacks;
  }
    return null;
  }

  @Override
  public boolean isCopyOnCreateFile() {
    return copyOnCreateFile;
  }

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/fs/Hdfs.java
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
    dfs.checkAccess(getUriPath(path), mode);
  }

  @Override
  public void setStoragePolicy(Path path, String policyName) throws IOException {
    dfs.setStoragePolicy(getUriPath(path), policyName);
  }

  @Override
  public Collection<? extends BlockStoragePolicySpi> getAllStoragePolicies()
      throws IOException {
    return Arrays.asList(dfs.getStoragePolicies());
  }


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/DistributedFileSystem.java
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.BlockStorageLocation;
import org.apache.hadoop.fs.BlockStoragePolicySpi;
import org.apache.hadoop.fs.CacheFlag;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.CreateFlag;
  @Override
  public void setStoragePolicy(final Path src, final String policyName)
      throws IOException {
    statistics.incrementWriteOps(1);
      @Override
      public Void next(final FileSystem fs, final Path p)
          throws IOException {
        fs.setStoragePolicy(p, policyName);
        return null;
      }
    }.resolve(this, absF);
  }

  @Override
  public Collection<BlockStoragePolicy> getAllStoragePolicies()
      throws IOException {
    return Arrays.asList(dfs.getStoragePolicies());
  }

  @Deprecated
  public BlockStoragePolicy[] getStoragePolicies() throws IOException {
    statistics.incrementReadOps(1);
    return dfs.getStoragePolicies();

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/mover/Mover.java
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.BlockStoragePolicySpi;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.HdfsConfiguration;
  }

  private void initStoragePolicies() throws IOException {
    Collection<BlockStoragePolicy> policies =
        dispatcher.getDistributedFileSystem().getAllStoragePolicies();
    for (BlockStoragePolicy policy : policies) {
      this.blockStoragePolicies[policy.getId()] = policy;
    }

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/tools/StoragePolicyAdmin.java

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

    public int run(Configuration conf, List<String> args) throws IOException {
      final DistributedFileSystem dfs = AdminHelper.getDFS(conf);
      try {
        Collection<BlockStoragePolicy> policies = dfs.getAllStoragePolicies();
        System.out.println("Block Storage Policies:");
        for (BlockStoragePolicy policy : policies) {
          if (policy != null) {
          System.out.println("The storage policy of " + path + " is unspecified");
          return 0;
        }
        Collection<BlockStoragePolicy> policies = dfs.getAllStoragePolicies();
        for (BlockStoragePolicy p : policies) {
          if (p.getId() == storagePolicyId) {
            System.out.println("The storage policy of " + path + ":\n" + p);

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestBlockStoragePolicy.java
import java.util.*;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockStoragePolicySpi;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
          policies[1].toString());
      Assert.assertEquals(POLICY_SUITE.getPolicy(HOT).toString(),
          policies[2].toString());
    } finally {
      IOUtils.cleanup(null, fs);
      cluster.shutdown();
      cluster.shutdown();
    }
  }

  @Test
  public void testGetAllStoragePoliciesFromFs() throws IOException {
    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(REPLICATION)
        .storageTypes(
            new StorageType[] {StorageType.DISK, StorageType.ARCHIVE})
        .build();
    try {
      cluster.waitActive();

      Set<String> policyNamesSet1 = new HashSet<>();
      for (BlockStoragePolicySpi policy :
          cluster.getFileSystem().getAllStoragePolicies()) {
        policyNamesSet1.add(policy.getName());
      }

      BlockStoragePolicySuite suite = BlockStoragePolicySuite.createDefaultSuite();
      Set<String> policyNamesSet2 = new HashSet<>();
      for (BlockStoragePolicy policy : suite.getAllPolicies()) {
        policyNamesSet2.add(policy.getName());
      }

      Assert.assertTrue(Sets.difference(policyNamesSet1, policyNamesSet2).isEmpty());
      Assert.assertTrue(Sets.difference(policyNamesSet2, policyNamesSet1).isEmpty());
    } finally {
      cluster.shutdown();
    }
  }
}

