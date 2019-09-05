hadoop-tools/hadoop-streaming/src/main/java/org/apache/hadoop/streaming/PipeMapRed.java
package org.apache.hadoop.streaming;

import java.io.*;
import java.util.Map.Entry;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Properties;
  void addJobConfToEnvironment(JobConf jobconf, Properties env) {
    JobConf conf = new JobConf(jobconf);
    conf.setDeprecatedProperties();
    int lenLimit = conf.getInt("stream.jobconf.truncate.limit", -1);

    for (Entry<String, String> confEntry: conf) {
      String name = confEntry.getKey();
      String value = conf.get(name); // does variable expansion
      name = safeEnvVarName(name);
      if (lenLimit > -1  && value.length() > lenLimit) {
        LOG.warn("Environment variable " + name + " truncated to " + lenLimit
            + " to  fit system limits.");
        value = value.substring(0, lenLimit);
      }
      envPut(env, name, value);
    }
  }

hadoop-tools/hadoop-streaming/src/main/java/org/apache/hadoop/streaming/StreamJob.java
        "/path/my-hadoop-streaming.jar");
    System.out.println("For more details about jobconf parameters see:");
    System.out.println("  http://wiki.apache.org/hadoop/JobConfFile");
    System.out.println("Truncate the values of the job configuration copied" +
        "to the environment at the given length:");
    System.out.println("   -D stream.jobconf.truncate.limit=-1");
    System.out.println("To set an environment variable in a streaming " +
        "command:");
    System.out.println("   -cmdenv EXAMPLE_DIR=/home/example/dictionaries/");
    System.out.println();

