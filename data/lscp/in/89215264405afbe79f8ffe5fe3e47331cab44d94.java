hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/tools/offlineImageViewer/PBImageDelimitedTextWriter.java
package org.apache.hadoop.hdfs.tools.offlineImageViewer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeSection.INode;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeSection.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeSection.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeSection.INodeSymlink;

import java.io.IOException;
import java.io.PrintStream;
import java.text.SimpleDateFormat;
  @Override
  public String getEntry(String parent, INode inode) {
    StringBuffer buffer = new StringBuffer();
    String inodeName = inode.getName().toStringUtf8();
    Path path = new Path(parent.isEmpty() ? "/" : parent,
      inodeName.isEmpty() ? "/" : inodeName);
    buffer.append(path.toString());
    PermissionStatus p = null;

    switch (inode.getType()) {

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/tools/offlineImageViewer/PBImageTextWriter.java
import com.google.common.collect.Lists;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.server.namenode.FSImageFormatPBINode;
import org.apache.hadoop.hdfs.server.namenode.FSImageFormatProtobuf;
          return "/";
        }
        if (this.path == null) {
          this.path = new Path(parent.getPath(), name.isEmpty() ? "/" : name).
              toString();
          this.name = null;
        }
        return this.path;
        }
        String parentName = toString(bytes);
        String parentPath =
            new Path(getParentPath(parent),
                parentName.isEmpty()? "/" : parentName).toString();
        dirPathCache.put(parent, parentPath);
      }
      return dirPathCache.get(parent);

