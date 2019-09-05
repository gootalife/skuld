hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-app/src/test/java/org/apache/hadoop/mapreduce/TestMapreduceConfigFields.java
++ b/hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-app/src/test/java/org/apache/hadoop/mapreduce/TestMapreduceConfigFields.java

package org.apache.hadoop.mapreduce;

import java.util.HashSet;

import org.apache.hadoop.conf.TestConfigurationFieldsBase;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.ShuffleHandler;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.v2.jobhistory.JHAdminConfig;

public class TestMapreduceConfigFields extends TestConfigurationFieldsBase {

  @Override
  public void initializeMemberVariables() {
    xmlFilename = new String("mapred-default.xml");
    configurationClasses = new Class[] { MRJobConfig.class, MRConfig.class,
        JHAdminConfig.class, ShuffleHandler.class, FileOutputFormat.class,
	FileInputFormat.class, Job.class, NLineInputFormat.class,
	JobConf.class, FileOutputCommitter.class };

    configurationPropsToSkipCompare = new HashSet<String>();
    xmlPropsToSkipCompare = new HashSet<String>();

    errorIfMissingConfigProps = true;
    errorIfMissingXmlProps = false;

    configurationPropsToSkipCompare
            .add(JobConf.MAPRED_JOB_MAP_MEMORY_MB_PROPERTY);
    configurationPropsToSkipCompare
            .add(JobConf.MAPRED_JOB_REDUCE_MEMORY_MB_PROPERTY);

    xmlPropsToSkipCompare.add("map.sort.class");
    xmlPropsToSkipCompare.add("mapreduce.reduce.skip.proc.count.autoincr");
    xmlPropsToSkipCompare.add("mapreduce.map.skip.proc.count.autoincr");
    xmlPropsToSkipCompare.add("mapreduce.local.clientfactory.class.name");
  }

}

