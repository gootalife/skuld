hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/DFSClient.java
    if (null == blockLocations) {
      throw new FileNotFoundException("File does not exist: " + src);
    }
    if (blockLocations.isUnderConstruction()) {
      throw new IOException("Fail to get checksum, since file " + src
          + " is under construction.");
    }
    List<LocatedBlock> locatedblocks = blockLocations.getLocatedBlocks();
    final DataOutputBuffer md5out = new DataOutputBuffer();
    int bytesPerCRC = -1;
        if (null == blockLocations) {
          throw new FileNotFoundException("File does not exist: " + src);
        }
        if (blockLocations.isUnderConstruction()) {
          throw new IOException("Fail to get checksum, since file " + src
              + " is under construction.");
        }
        locatedblocks = blockLocations.getLocatedBlocks();
        refetchBlocks = false;
      }

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestGetFileChecksum.java
package org.apache.hadoop.hdfs;

import static org.junit.Assert.fail;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.Path;
import org.junit.After;
    }
  }

  @Test
  public void testGetFileChecksumForBlocksUnderConstruction() {
    try {
      FSDataOutputStream file = dfs.create(new Path("/testFile"));
      file.write("Performance Testing".getBytes());
      dfs.getFileChecksum(new Path("/testFile"));
      fail("getFileChecksum should fail for files "
          + "with blocks under construction");
    } catch (IOException ie) {
      Assert.assertTrue(ie.getMessage().contains(
          "Fail to get checksum, since file /testFile "
              + "is under construction."));
    }
  }
  @Test
  public void testGetFileChecksum() throws Exception {
    testGetFileChecksum(new Path("/foo"), BLOCKSIZE / 4);

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/snapshot/TestSnapshotFileLength.java
package org.apache.hadoop.hdfs.server.namenode.snapshot;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;

import org.apache.hadoop.fs.FSDataOutputStream;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
    FSDataOutputStream out = hdfs.append(file1);
    try {
      hdfs.getFileChecksum(file1);
      fail("getFileChecksum should fail for files "
          + "with blocks under construction");
    } catch (IOException ie) {
      assertTrue(ie.getMessage().contains(
          "Fail to get checksum, since file " + file1
              + " is under construction."));
    }
    assertThat("snapshot checksum (post-open for append) has changed",
        hdfs.getFileChecksum(file1snap1), is(snapChksum1));
    try {
      assertThat("Wrong data size in snapshot.",
          dataFromSnapshot.length, is(origLen));
      assertThat("snapshot checksum (post-append) has changed",
          hdfs.getFileChecksum(file1snap1), is(snapChksum1));
    } finally {

