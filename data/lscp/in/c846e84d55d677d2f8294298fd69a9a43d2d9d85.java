hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/AclStorage.java
    return getEntriesFromAclFeature(f);
  }

  public static List<AclEntry> readINodeAcl(INodeAttributes inodeAttr) {
    AclFeature f = inodeAttr.getAclFeature();
    return getEntriesFromAclFeature(f);
  }


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSDirAclOp.java
      }
      INode inode = FSDirectory.resolveLastINode(iip);
      int snapshotId = iip.getPathSnapshotId();
      List<AclEntry> acl = AclStorage.readINodeAcl(fsd.getAttributes(src,
              inode.getLocalNameBytes(), inode, snapshotId));
      FsPermission fsPermission = inode.getFsPermission(snapshotId);
      return new AclStatus.Builder()
          .owner(inode.getUserName()).group(inode.getGroupName())

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSDirXAttrOp.java
      INodesInPath iip = fsd.getINodesInPath(srcs, true);
      INode inode = FSDirectory.resolveLastINode(iip);
      int snapshotId = iip.getPathSnapshotId();
      return XAttrStorage.readINodeXAttrs(fsd.getAttributes(src,
              inode.getLocalNameBytes(), inode, snapshotId));
    } finally {
      fsd.readUnlock();
    }

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/XAttrStorage.java
  public static List<XAttr> readINodeXAttrs(INodeAttributes inodeAttr) {
    XAttrFeature f = inodeAttr.getXAttrFeature();
    return f == null ? ImmutableList.<XAttr> of() : f.getXAttrs();
  }
  

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/TestINodeAttributeProvider.java
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.permission.*;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;

        @Override
        public XAttrFeature getXAttrFeature() {
          XAttrFeature x;
          if (useDefault) {
            x = inode.getXAttrFeature();
          } else {
            x = new XAttrFeature(ImmutableList.copyOf(
                    Lists.newArrayList(
                            new XAttr.Builder().setName("test")
                                    .setValue(new byte[] {1, 2})
                                    .build())));
          }
          return x;
        }

        @Override
    Assert.assertEquals("supergroup", status.getGroup());
    Assert.assertEquals(new FsPermission((short) 0755), status.getPermission());
    fs.mkdirs(new Path("/user/authz"));
    Path p = new Path("/user/authz");
    status = fs.getFileStatus(p);
    Assert.assertEquals("foo", status.getOwner());
    Assert.assertEquals("bar", status.getGroup());
    Assert.assertEquals(new FsPermission((short) 0770), status.getPermission());
    AclStatus aclStatus = fs.getAclStatus(p);
    Assert.assertEquals(1, aclStatus.getEntries().size());
    Assert.assertEquals(AclEntryType.GROUP, aclStatus.getEntries().get(0)
            .getType());
    Assert.assertEquals("xxx", aclStatus.getEntries().get(0)
            .getName());
    Assert.assertEquals(FsAction.ALL, aclStatus.getEntries().get(0)
            .getPermission());
    Map<String, byte[]> xAttrs = fs.getXAttrs(p);
    Assert.assertTrue(xAttrs.containsKey("user.test"));
    Assert.assertEquals(2, xAttrs.get("user.test").length);
  }

}

