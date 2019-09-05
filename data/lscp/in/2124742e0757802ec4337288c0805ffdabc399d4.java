hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/tools/TestHdfsConfigFields.java
++ b/hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/tools/TestHdfsConfigFields.java

package org.apache.hadoop.hdfs.tools;

import java.util.HashSet;

import org.apache.hadoop.conf.TestConfigurationFieldsBase;
import org.apache.hadoop.hdfs.DFSConfigKeys;

public class TestHdfsConfigFields extends TestConfigurationFieldsBase {

  @Override
  public void initializeMemberVariables() {
    xmlFilename = new String("hdfs-default.xml");
    configurationClasses = new Class[] { DFSConfigKeys.class };

    errorIfMissingConfigProps = true;
    errorIfMissingXmlProps = false;

    xmlPropsToSkipCompare = new HashSet<String>();
    xmlPrefixToSkipCompare = new HashSet<String>();

    xmlPropsToSkipCompare.add("hadoop.fuse.timer.period");
    xmlPropsToSkipCompare.add("hadoop.fuse.connection.timeout");

    xmlPropsToSkipCompare.add("dfs.namenode.edits.journal-plugin.qjournal");

    xmlPropsToSkipCompare.add("dfs.ha.namenodes.EXAMPLENAMESERVICE");

    xmlPropsToSkipCompare.add("hadoop.user.group.metrics.percentiles.intervals");

    xmlPropsToSkipCompare.add("hadoop.hdfs.configuration.version");

    xmlPrefixToSkipCompare.add("nfs");

    xmlPrefixToSkipCompare.add("dfs.namenode.kerberos.principal.pattern");

    xmlPropsToSkipCompare.add("dfs.webhdfs.enabled");

    xmlPropsToSkipCompare.add("dfs.client.short.circuit.replica.stale.threshold.ms");
  }
}

