hadoop-tools/hadoop-sls/src/main/java/org/apache/hadoop/yarn/sls/RumenToSLSConverter.java
package org.apache.hadoop.yarn.sls;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.util.ArrayList;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.sls.utils.SLSUtils;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectWriter;

@Private
@Unstable
public class RumenToSLSConverter {

  private static void generateSLSLoadFile(String inputFile, String outputFile)
          throws IOException {
    try (Reader input =
        new InputStreamReader(new FileInputStream(inputFile), "UTF-8")) {
      try (Writer output =
          new OutputStreamWriter(new FileOutputStream(outputFile), "UTF-8")) {
        ObjectMapper mapper = new ObjectMapper();
        ObjectWriter writer = mapper.writerWithDefaultPrettyPrinter();
        Iterator<Map> i = mapper.readValues(
          Map m = i.next();
          output.write(writer.writeValueAsString(createSLSJob(m)) + EOL);
        }
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static void generateSLSNodeFile(String outputFile)
          throws IOException {
    try (Writer output =
        new OutputStreamWriter(new FileOutputStream(outputFile), "UTF-8")) {
      ObjectMapper mapper = new ObjectMapper();
      ObjectWriter writer = mapper.writerWithDefaultPrettyPrinter();
      for (Map.Entry<String, Set<String>> entry : rackNodeMap.entrySet()) {
        rack.put("nodes", nodes);
        output.write(writer.writeValueAsString(rack) + EOL);
      }
    }
  }


hadoop-tools/hadoop-sls/src/main/java/org/apache/hadoop/yarn/sls/SLSRunner.java
package org.apache.hadoop.yarn.sls;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.tools.rumen.JobTraceReader;
import org.apache.hadoop.tools.rumen.LoggedJob;
import org.apache.hadoop.tools.rumen.LoggedTask;
import org.apache.hadoop.tools.rumen.LoggedTaskAttempt;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.sls.appmaster.AMSimulator;
import org.apache.hadoop.yarn.sls.conf.SLSConfiguration;
import org.apache.hadoop.yarn.sls.nodemanager.NMSimulator;
import org.apache.hadoop.yarn.sls.scheduler.ContainerSimulator;
import org.apache.hadoop.yarn.sls.scheduler.ResourceSchedulerWrapper;
import org.apache.hadoop.yarn.sls.scheduler.TaskRunner;
import org.apache.hadoop.yarn.sls.utils.SLSUtils;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonFactory;
    JsonFactory jsonF = new JsonFactory();
    ObjectMapper mapper = new ObjectMapper();
    for (String inputTrace : inputTraces) {
      Reader input =
          new InputStreamReader(new FileInputStream(inputTrace), "UTF-8");
      try {
        Iterator<Map> i = mapper.readValues(jsonF.createJsonParser(input),
                Map.class);

hadoop-tools/hadoop-sls/src/main/java/org/apache/hadoop/yarn/sls/scheduler/ResourceSchedulerWrapper.java

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
            TimeUnit.MILLISECONDS);

    jobRuntimeLogBW =
        new BufferedWriter(new OutputStreamWriter(new FileOutputStream(
            metricsOutputDir + "/jobruntime.csv"), "UTF-8"));
    jobRuntimeLogBW.write("JobID,real_start_time,real_end_time," +
            "simulate_start_time,simulate_end_time" + EOL);
    jobRuntimeLogBW.flush();
    private boolean firstLine = true;
    public MetricsLogRunnable() {
      try {
        metricsLogBW =
            new BufferedWriter(new OutputStreamWriter(new FileOutputStream(
                metricsOutputDir + "/realtimetrack.json"), "UTF-8"));
        metricsLogBW.write("[");
      } catch (IOException e) {
        e.printStackTrace();

hadoop-tools/hadoop-sls/src/main/java/org/apache/hadoop/yarn/sls/utils/SLSUtils.java
package org.apache.hadoop.yarn.sls.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.map.ObjectMapper;

@Private
@Unstable
public class SLSUtils {
    Set<String> nodeSet = new HashSet<String>();
    JsonFactory jsonF = new JsonFactory();
    ObjectMapper mapper = new ObjectMapper();
    Reader input =
        new InputStreamReader(new FileInputStream(jobTrace), "UTF-8");
    try {
      Iterator<Map> i = mapper.readValues(
              jsonF.createJsonParser(input), Map.class);
    Set<String> nodeSet = new HashSet<String>();
    JsonFactory jsonF = new JsonFactory();
    ObjectMapper mapper = new ObjectMapper();
    Reader input =
        new InputStreamReader(new FileInputStream(nodeFile), "UTF-8");
    try {
      Iterator<Map> i = mapper.readValues(
              jsonF.createJsonParser(input), Map.class);

hadoop-tools/hadoop-sls/src/main/java/org/apache/hadoop/yarn/sls/web/SLSWebApp.java

import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEventType;
import org.apache.hadoop.yarn.sls.SLSRunner;
import org.apache.hadoop.yarn.sls.scheduler.FairSchedulerMetrics;
import org.apache.hadoop.yarn.sls.scheduler.ResourceSchedulerWrapper;
import org.apache.hadoop.yarn.sls.scheduler.SchedulerMetrics;
import org.mortbay.jetty.Handler;
import org.mortbay.jetty.Request;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.handler.AbstractHandler;
import org.mortbay.jetty.handler.ResourceHandler;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;

@Private
@Unstable
  private transient Gauge availableVCoresGauge;
  private transient Histogram allocateTimecostHistogram;
  private transient Histogram handleTimecostHistogram;
  private transient Map<SchedulerEventType, Histogram>
     handleOperTimecostHistogramMap;
  private transient Map<String, Counter> queueAllocatedMemoryCounterMap;
  private transient Map<String, Counter> queueAllocatedVCoresCounterMap;
  private int port;
  private int ajaxUpdateTimeMS = 1000;
    }
  }

  private void readObject(ObjectInputStream in) throws IOException,
      ClassNotFoundException {
    in.defaultReadObject();
    handleOperTimecostHistogramMap = new HashMap<>();
    queueAllocatedMemoryCounterMap = new HashMap<>();
    queueAllocatedVCoresCounterMap = new HashMap<>();
  }

  public SLSWebApp(ResourceSchedulerWrapper wrapper, int metricsAddressPort) {
    this.wrapper = wrapper;
    metrics = wrapper.getMetrics();

