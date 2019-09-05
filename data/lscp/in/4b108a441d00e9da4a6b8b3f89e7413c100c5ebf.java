hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/tracing/SpanReceiverHost.java
package org.apache.hadoop.tracing;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tracing.SpanReceiverInfo.ConfigurationPair;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.htrace.SpanReceiver;
import org.apache.htrace.SpanReceiverBuilder;
import org.apache.htrace.Trace;
import org.apache.htrace.impl.LocalFileSpanReceiver;


  private static List<ConfigurationPair> EMPTY = Collections.emptyList();

  private SpanReceiverHost(String confPrefix) {
    this.confPrefix = confPrefix;
  }
    String pathKey = confPrefix + LOCAL_FILE_SPAN_RECEIVER_PATH_SUFFIX;
    if (config.get(pathKey) == null) {
      String uniqueFile = LocalFileSpanReceiver.getUniqueLocalTraceFileName();
      config.set(pathKey, uniqueFile);
      if (LOG.isTraceEnabled()) {
        LOG.trace("Set " + pathKey + " to " + uniqueFile);

  public synchronized SpanReceiverInfo[] listSpanReceivers()
      throws IOException {
    SpanReceiverInfo[] info = new SpanReceiverInfo[receivers.size()];
    int i = 0;

    for(Map.Entry<Long, SpanReceiver> entry : receivers.entrySet()) {

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/DFSClient.java
import java.net.SocketAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Collections;
    return saslClient;
  }

  TraceScope getPathTraceScope(String description, String path) {
    TraceScope scope = Trace.startSpan(description, traceSampler);
    Span span = scope.getSpan();
    if (span != null) {
      if (path != null) {
        span.addKVAnnotation("path", path);
      }
    }
    return scope;
  }

  TraceScope getSrcDstTraceScope(String description, String src, String dst) {
    TraceScope scope = Trace.startSpan(description, traceSampler);
    Span span = scope.getSpan();
    if (span != null) {
      if (src != null) {
        span.addKVAnnotation("src", src);
      }
      if (dst != null) {
        span.addKVAnnotation("dst", dst);
      }
    }
    return scope;

