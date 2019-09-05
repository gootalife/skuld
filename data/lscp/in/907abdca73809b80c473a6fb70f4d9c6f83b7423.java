hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/tools/DFSAdmin.java
      String storageTypeString =
          StringUtils.popOptionWithArgument("-storageType", parameters);
      if (storageTypeString != null) {
        try {
          this.type = StorageType.parseStorageType(storageTypeString);
        } catch (IllegalArgumentException e) {
          throw new IllegalArgumentException("Storage type "
              + storageTypeString
              + " is not available. Available storage types are "
              + StorageType.getTypesSupportingQuota());
        }
      }
      this.args = parameters.toArray(new String[parameters.size()]);
    }
    

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestQuota.java
package org.apache.hadoop.hdfs;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.security.PrivilegedExceptionAction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.DSQuotaExceededException;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.base.Charsets;

public class TestQuota {
  
      cluster.shutdown();
    }
  }

  @Test
  public void testSetSpaceQuotaWhenStorageTypeIsWrong() throws Exception {
    Configuration conf = new HdfsConfiguration();
    conf.set(FS_DEFAULT_NAME_KEY, "hdfs://127.0.0.1:8020");
    DFSAdmin admin = new DFSAdmin(conf);
    ByteArrayOutputStream err = new ByteArrayOutputStream();
    System.setErr(new PrintStream(err));
    String[] args = { "-setSpaceQuota", "100", "-storageType", "COLD",
        "/testDir" };
    admin.run(args);
    String errOutput = new String(err.toByteArray(), Charsets.UTF_8);
    assertTrue(errOutput.contains(StorageType.getTypesSupportingQuota()
        .toString()));
  }
}

