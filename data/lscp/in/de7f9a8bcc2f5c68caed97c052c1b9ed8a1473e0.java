hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/fs/Hdfs.java
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.Options.ChecksumOpt;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSInputStream;
import org.apache.hadoop.hdfs.DFSOutputStream;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.client.HdfsDataInputStream;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.apache.hadoop.hdfs.client.impl.CorruptFileBlockIterator;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/DistributedFileSystem.java
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.client.HdfsAdmin;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.apache.hadoop.hdfs.client.impl.CorruptFileBlockIterator;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveEntry;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveInfo;

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/client/impl/CorruptFileBlockIterator.java
++ b/hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/client/impl/CorruptFileBlockIterator.java

package org.apache.hadoop.hdfs.client.impl;

import java.io.IOException;
import java.util.NoSuchElementException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.protocol.CorruptFileBlocks;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

@InterfaceAudience.Private
public class CorruptFileBlockIterator implements RemoteIterator<Path> {
  private final DFSClient dfs;
  private final String path;

  private String[] files = null;
  private int fileIdx = 0;
  private String cookie = null;
  private Path nextPath = null;

  private int callsMade = 0;

  public CorruptFileBlockIterator(DFSClient dfs, Path path) throws IOException {
    this.dfs = dfs;
    this.path = path2String(path);
    loadNext();
  }

  public int getCallsMade() {
    return callsMade;
  }

  private String path2String(Path path) {
    return path.toUri().getPath();
  }

  private Path string2Path(String string) {
    return new Path(string);
  }

  private void loadNext() throws IOException {
    if (files == null || fileIdx >= files.length) {
      CorruptFileBlocks cfb = dfs.listCorruptFileBlocks(path, cookie);
      files = cfb.getFiles();
      cookie = cfb.getCookie();
      fileIdx = 0;
      callsMade++;
    }

    if (fileIdx >= files.length) {
      nextPath = null;
    } else {
      nextPath = string2Path(files[fileIdx]);
      fileIdx++;
    }
  }

  
  @Override
  public boolean hasNext() {
    return nextPath != null;
  }

  
  @Override
  public Path next() throws IOException {
    if (!hasNext()) {
      throw new NoSuchElementException("No more corrupt file blocks");
    }

    Path result = nextPath;
    loadNext();

    return result;
  }
}
\No newline at end of file

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/TestListCorruptFileBlocks.java
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.BlockMissingException;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.client.impl.CorruptFileBlockIterator;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.datanode.DataNode;

