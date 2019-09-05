hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/DFSConfigKeys.java
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementPolicyDefault;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.RamDiskReplicaLruTracker;
import org.apache.hadoop.http.HttpConfig;

  public static final String  DFS_NAMENODE_REPLICATION_STREAMS_HARD_LIMIT_KEY = "dfs.namenode.replication.max-streams-hard-limit";
  public static final int     DFS_NAMENODE_REPLICATION_STREAMS_HARD_LIMIT_DEFAULT = 4;
  public static final String  DFS_WEBHDFS_AUTHENTICATION_FILTER_KEY = "dfs.web.authentication.filter";
     this was AuthFilter.class.getName(). Note that if you change the import for AuthFilter, you
     need to update the literal here as well as TestDFSConfigKeys.
  public static final String  DFS_WEBHDFS_AUTHENTICATION_FILTER_DEFAULT =
      "org.apache.hadoop.hdfs.web.AuthFilter".toString();
  public static final String  DFS_WEBHDFS_ENABLED_KEY = "dfs.webhdfs.enabled";
  public static final boolean DFS_WEBHDFS_ENABLED_DEFAULT = true;
  public static final String  DFS_WEBHDFS_USER_PATTERN_KEY = "dfs.webhdfs.user.provider.user.pattern";

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestDFSConfigKeys.java
++ b/hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestDFSConfigKeys.java
package org.apache.hadoop.hdfs;

import org.apache.hadoop.hdfs.web.AuthFilter;

import org.junit.Assert;
import org.junit.Test;

public class TestDFSConfigKeys {

  @Test
  public void testStringLiteralDefaultWebFilter() {
    Assert.assertEquals("The default webhdfs auth filter should make the FQCN of AuthFilter.",
        AuthFilter.class.getName(), DFSConfigKeys.DFS_WEBHDFS_AUTHENTICATION_FILTER_DEFAULT);
  }
 
}

