hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/tools/offlineImageViewer/PBImageXmlWriter.java

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.CacheDirectiveInfoExpirationProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.CacheDirectiveInfoProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.CachePoolInfoProto;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.FilesUnderConstructionSection.FileUnderConstructionEntry;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeDirectorySection;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeSection;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeSection.AclFeatureProto;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeSection.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeSection.INodeSymlink;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeReferenceSection;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.StringTableSection;
import org.apache.hadoop.hdfs.util.XMLUtils;
import org.apache.hadoop.util.LimitInputStream;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

  private void dumpINodeDirectory(INodeDirectory d) {
    o("mtime", d.getModificationTime()).o("permission",
        dumpPermission(d.getPermission()));
    dumpAcls(d.getAcl());
    if (d.hasDsQuota() && d.hasNsQuota()) {
      o("nsquota", d.getNsQuota()).o("dsquota", d.getDsQuota());
    }
        .o("atime", f.getAccessTime())
        .o("perferredBlockSize", f.getPreferredBlockSize())
        .o("permission", dumpPermission(f.getPermission()));
    dumpAcls(f.getAcl());
    if (f.getBlocksCount() > 0) {
      out.print("<blocks>");
      for (BlockProto b : f.getBlocksList()) {
    }
  }

  private void dumpAcls(AclFeatureProto aclFeatureProto) {
    ImmutableList<AclEntry> aclEntryList = FSImageFormatPBINode.Loader
        .loadAclEntries(aclFeatureProto, stringTable);
    if (aclEntryList.size() > 0) {
      out.print("<acls>");
      for (AclEntry aclEntry : aclEntryList) {
        o("acl", aclEntry.toString());
      }
      out.print("</acls>");
    }
  }

  private void dumpINodeSection(InputStream in) throws IOException {
    INodeSection s = INodeSection.parseDelimitedFrom(in);
    out.print("<INodeSection>");

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/tools/offlineImageViewer/TestOfflineImageViewerForAcl.java
package org.apache.hadoop.hdfs.tools.offlineImageViewer;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.io.RandomAccessFile;
import java.io.StringReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.HashMap;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import static org.apache.hadoop.fs.permission.AclEntryScope.ACCESS;
import static org.apache.hadoop.fs.permission.AclEntryScope.DEFAULT;
      viewer.close();
    }
  }

  @Test
  public void testPBImageXmlWriterForAcl() throws Exception{
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    PrintStream o = new PrintStream(output);
    PBImageXmlWriter v = new PBImageXmlWriter(new Configuration(), o);
    v.visit(new RandomAccessFile(originalFsimage, "r"));
    SAXParserFactory spf = SAXParserFactory.newInstance();
    SAXParser parser = spf.newSAXParser();
    final String xml = output.toString();
    parser.parse(new InputSource(new StringReader(xml)), new DefaultHandler());
  }
}

