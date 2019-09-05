hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-jobclient/src/main/java/org/apache/hadoop/mapred/ResourceMgrDelegate.java
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerReport;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
  }

  @Override
  public List<NodeLabel> getClusterNodeLabels()
      throws YarnException, IOException {
    return client.getClusterNodeLabels();
  }

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-api/src/main/java/org/apache/hadoop/yarn/api/protocolrecords/GetClusterNodeLabelsResponse.java

package org.apache.hadoop.yarn.api.protocolrecords;

import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.util.Records;

@Public
@Evolving
public abstract class GetClusterNodeLabelsResponse {
  public static GetClusterNodeLabelsResponse newInstance(List<NodeLabel> labels) {
    GetClusterNodeLabelsResponse request =
        Records.newRecord(GetClusterNodeLabelsResponse.class);
    request.setNodeLabels(labels);

  @Public
  @Evolving
  public abstract void setNodeLabels(List<NodeLabel> labels);

  @Public
  @Evolving
  public abstract List<NodeLabel> getNodeLabels();
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-api/src/main/java/org/apache/hadoop/yarn/api/records/NodeLabel.java

package org.apache.hadoop.yarn.api.records;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.classification.InterfaceStability.Unstable;

@Public
@Unstable
public abstract class NodeLabel implements Comparable<NodeLabel> {
  @Private
  @Unstable
  public static final boolean DEFAULT_NODE_LABEL_EXCLUSIVITY = true;

  @Private
  @Unstable
  public static NodeLabel newInstance(String name) {
    return newInstance(name, DEFAULT_NODE_LABEL_EXCLUSIVITY);
  }

  @Private
  @Unstable
  public static NodeLabel newInstance(String name, boolean isExclusive) {
    NodeLabel request = Records.newRecord(NodeLabel.class);
    request.setName(name);
    request.setExclusivity(isExclusive);
    return request;
  }

  @Public
  @Stable
  public abstract String getName();

  @Private
  @Unstable
  public abstract void setName(String name);

  @Public
  @Stable
  public abstract boolean isExclusive();

  @Private
  @Unstable
  public abstract void setExclusivity(boolean isExclusive);

  @Override
  public int compareTo(NodeLabel other) {
    return getName().compareTo(other.getName());
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof NodeLabel) {
      NodeLabel nl = (NodeLabel) obj;
      return nl.getName().equals(getName())
          && nl.isExclusive() == isExclusive();
    }
    return false;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("<");
    sb.append(getName());
    sb.append(":exclusivity=");
    sb.append(isExclusive());
    sb.append(">");
    return sb.toString();
  }

  @Override
  public int hashCode() {
    return (getName().hashCode() << 16) + (isExclusive() ? 1 : 0);
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-api/src/main/java/org/apache/hadoop/yarn/server/api/ResourceManagerAdministrationProtocol.java
import org.apache.hadoop.yarn.server.api.protocolrecords.RemoveFromClusterNodeLabelsResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.ReplaceLabelsOnNodeRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.ReplaceLabelsOnNodeResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.UpdateNodeResourceRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.UpdateNodeResourceResponse;

  public ReplaceLabelsOnNodeResponse replaceLabelsOnNode(
      ReplaceLabelsOnNodeRequest request) throws YarnException, IOException;
  
  @Public
  @Evolving
  @Idempotent

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-api/src/main/java/org/apache/hadoop/yarn/server/api/protocolrecords/AddToClusterNodeLabelsRequest.java

package org.apache.hadoop.yarn.server.api.protocolrecords;

import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.util.Records;

@Public
@Unstable
public abstract class AddToClusterNodeLabelsRequest {
  @Public
  @Unstable
  public static AddToClusterNodeLabelsRequest newInstance(
      List<NodeLabel> NodeLabels) {
    AddToClusterNodeLabelsRequest request =
      Records.newRecord(AddToClusterNodeLabelsRequest.class);
    request.setNodeLabels(NodeLabels);
    return request;
  }

  @Public
  @Unstable
  public abstract void setNodeLabels(List<NodeLabel> NodeLabels);

  @Public
  @Unstable
  public abstract List<NodeLabel> getNodeLabels();
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-applications/hadoop-yarn-applications-distributedshell/src/test/java/org/apache/hadoop/yarn/applications/distributedshell/TestDistributedShellWithNodeLabels.java
    RMNodeLabelsManager labelsMgr = rmContext.getNodeLabelManager();
    Set<String> labels = new HashSet<String>();
    labels.add("x");
    labelsMgr.addToCluserNodeLabelsWithDefaultExclusivity(labels);

    distShellTest.conf.set("yarn.scheduler.capacity.root.accessible-node-labels", "x");

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-client/src/main/java/org/apache/hadoop/yarn/client/api/YarnClient.java
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerReport;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.QueueInfo;
  @Public
  @Unstable
  public abstract List<NodeLabel> getClusterNodeLabels()
      throws YarnException, IOException;
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-client/src/main/java/org/apache/hadoop/yarn/client/api/impl/YarnClientImpl.java
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerReport;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.QueueInfo;
  }

  @Override
  public List<NodeLabel> getClusterNodeLabels() throws YarnException, IOException {
    return rmClient.getClusterNodeLabels(
        GetClusterNodeLabelsRequest.newInstance()).getNodeLabels();
  }

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-client/src/main/java/org/apache/hadoop/yarn/client/cli/ClusterCLI.java
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
    return 0;
  }

  void printClusterNodeLabels() throws YarnException, IOException {
    List<NodeLabel> nodeLabels = null;
    if (accessLocal) {
      nodeLabels =
          new ArrayList<>(getNodeLabelManagerInstance(getConf()).getClusterNodeLabels());
    } else {
      nodeLabels = new ArrayList<>(client.getClusterNodeLabels());
    }
    sysout.println(String.format("Node Labels: %s",
        StringUtils.join(nodeLabels.iterator(), ",")));
  }

  @VisibleForTesting

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-client/src/main/java/org/apache/hadoop/yarn/client/cli/RMAdminCLI.java
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.api.records.DecommissionType;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.client.ClientRMProxy;
import org.apache.hadoop.yarn.client.RMHAServiceTarget;
import org.apache.hadoop.yarn.conf.HAUtil;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

@Private
@Unstable
      "No node-to-labels mappings are specified";
  private static final String INVALID_TIMEOUT_ERR_MSG =
      "Invalid timeout specified : ";
  private static final String ADD_LABEL_FORMAT_ERR_MSG =
      "Input format for adding node-labels is not correct, it should be "
          + "labelName1[(exclusive=true/false)],LabelName2[] ..";

  protected final static Map<String, UsageInfo> ADMIN_USAGE =
      ImmutableMap.<String, UsageInfo>builder()
          .put("-getGroups", new UsageInfo("[username]",
              "Get the groups which given user belongs to."))
          .put("-addToClusterNodeLabels",
              new UsageInfo("[label1(exclusive=true),"
                  + "label2(exclusive=false),label3]",
                  "add to cluster node labels "))
          .put("-removeFromClusterNodeLabels",
              new UsageInfo("[label1,label2,label3] (label splitted by \",\")",
    return localNodeLabelsManager;
  }
  
  private List<NodeLabel> buildNodeLabelsFromStr(String args) {
    List<NodeLabel> nodeLabels = new ArrayList<>();
    for (String p : args.split(",")) {
      if (!p.trim().isEmpty()) {
        String labelName = p;

        boolean exclusive = NodeLabel.DEFAULT_NODE_LABEL_EXCLUSIVITY;
        int leftParenthesisIdx = p.indexOf("(");
        int rightParenthesisIdx = p.indexOf(")");

        if ((leftParenthesisIdx == -1 && rightParenthesisIdx != -1)
            || (leftParenthesisIdx != -1 && rightParenthesisIdx == -1)) {
          throw new IllegalArgumentException(ADD_LABEL_FORMAT_ERR_MSG);
        }

        if (leftParenthesisIdx > 0 && rightParenthesisIdx > 0) {
          if (leftParenthesisIdx > rightParenthesisIdx) {
            throw new IllegalArgumentException(ADD_LABEL_FORMAT_ERR_MSG);
          }

          String property = p.substring(p.indexOf("(") + 1, p.indexOf(")"));
          if (property.contains("=")) {
            String key = property.substring(0, property.indexOf("=")).trim();
            String value =
                property
                    .substring(property.indexOf("=") + 1, property.length())
                    .trim();

            if (key.equals("exclusive")
                && ImmutableSet.of("true", "false").contains(value)) {
              exclusive = Boolean.parseBoolean(value);
            } else {
              throw new IllegalArgumentException(ADD_LABEL_FORMAT_ERR_MSG);
            }
          } else if (!property.trim().isEmpty()) {
            throw new IllegalArgumentException(ADD_LABEL_FORMAT_ERR_MSG);
          }
        }

        if (labelName.contains("(")) {
          labelName = labelName.substring(0, labelName.indexOf("(")).trim();
        }

        nodeLabels.add(NodeLabel.newInstance(labelName, exclusive));
      }
    }

    if (nodeLabels.isEmpty()) {
      throw new IllegalArgumentException(NO_LABEL_ERR_MSG);
    }
    return nodeLabels;
  }

  private Set<String> buildNodeLabelNamesFromStr(String args) {
    Set<String> labels = new HashSet<String>();
    for (String p : args.split(",")) {
      if (!p.trim().isEmpty()) {

  private int addToClusterNodeLabels(String args) throws IOException,
      YarnException {
    List<NodeLabel> labels = buildNodeLabelsFromStr(args);

    if (directlyAccessNodeLabelStore) {
      getNodeLabelManagerInstance(getConf()).addToCluserNodeLabels(labels);

  private int removeFromClusterNodeLabels(String args) throws IOException,
      YarnException {
    Set<String> labels = buildNodeLabelNamesFromStr(args);

    if (directlyAccessNodeLabelStore) {
      getNodeLabelManagerInstance(getConf()).removeFromClusterNodeLabels(

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-client/src/test/java/org/apache/hadoop/yarn/client/cli/TestClusterCLI.java
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.junit.Before;
  public void testGetClusterNodeLabels() throws Exception {
    YarnClient client = mock(YarnClient.class);
    when(client.getClusterNodeLabels()).thenReturn(
        Arrays.asList(NodeLabel.newInstance("label1"),
            NodeLabel.newInstance("label2")));
    ClusterCLI cli = new ClusterCLI();
    cli.setClient(client);
    cli.setSysOutPrintStream(sysOut);
    
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintWriter pw = new PrintWriter(baos);
    pw.print("Node Labels: <label1:exclusivity=true>,<label2:exclusivity=true>");
    pw.close();
    verify(sysOut).println(baos.toString("UTF-8"));
  }
  public void testGetClusterNodeLabelsWithLocalAccess() throws Exception {
    YarnClient client = mock(YarnClient.class);
    when(client.getClusterNodeLabels()).thenReturn(
        Arrays.asList(NodeLabel.newInstance("remote1"),
            NodeLabel.newInstance("remote2")));
    ClusterCLI cli = new ClusterCLI();
    cli.setClient(client);
    cli.setSysOutPrintStream(sysOut);
    cli.setSysErrPrintStream(sysErr);
    ClusterCLI.localNodeLabelsManager = mock(CommonNodeLabelsManager.class);
    when(ClusterCLI.localNodeLabelsManager.getClusterNodeLabels()).thenReturn(
        Arrays.asList(NodeLabel.newInstance("local1"),
            NodeLabel.newInstance("local2")));

    int rc =
        cli.run(new String[] { ClusterCLI.CMD,
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintWriter pw = new PrintWriter(baos);
    pw.print("Node Labels: <local1:exclusivity=true>,<local2:exclusivity=true>");
    pw.close();
    verify(sysOut).println(baos.toString("UTF-8"));
  }
  @Test
  public void testGetEmptyClusterNodeLabels() throws Exception {
    YarnClient client = mock(YarnClient.class);
    when(client.getClusterNodeLabels()).thenReturn(new ArrayList<NodeLabel>());
    ClusterCLI cli = new ClusterCLI();
    cli.setClient(client);
    cli.setSysOutPrintStream(sysOut);

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-client/src/test/java/org/apache/hadoop/yarn/client/cli/TestRMAdminCLI.java
    String[] args =
        { "-addToClusterNodeLabels", "x,y", "-directlyAccessNodeLabelStore" };
    assertEquals(0, rmAdminCLI.run(args));
    assertTrue(dummyNodeLabelsManager.getClusterNodeLabelNames().containsAll(
        ImmutableSet.of("x", "y")));
    
        new String[] { "-addToClusterNodeLabels",
            "-directlyAccessNodeLabelStore", "x,y" };
    assertEquals(0, rmAdminCLI.run(args));
    assertTrue(dummyNodeLabelsManager.getClusterNodeLabelNames().containsAll(
        ImmutableSet.of("x", "y")));
    
    assertEquals(0, rmAdminCLI.run(args));
    
    assertTrue(dummyNodeLabelsManager.getClusterNodeLabelNames().isEmpty());
    
    assertTrue(remoteAdminServiceAccessed);
    String[] args =
        { "-addToClusterNodeLabels", "x", "-directlyAccessNodeLabelStore" };
    assertEquals(0, rmAdminCLI.run(args));
    assertTrue(dummyNodeLabelsManager.getClusterNodeLabelNames().containsAll(
        ImmutableSet.of("x")));
    
        new String[] { "-addToClusterNodeLabels", ",x,,",
            "-directlyAccessNodeLabelStore" };
    assertEquals(0, rmAdminCLI.run(args));
    assertTrue(dummyNodeLabelsManager.getClusterNodeLabelNames().containsAll(
        ImmutableSet.of("x")));
  }
  
  @Test
  public void testAddToClusterNodeLabelsWithExclusivitySetting()
      throws Exception {
    String[] args = new String[] { "-addToClusterNodeLabels", "x(" };
    assertTrue(0 != rmAdminCLI.run(args));

    args = new String[] { "-addToClusterNodeLabels", "x)" };
    assertTrue(0 != rmAdminCLI.run(args));

    args = new String[] { "-addToClusterNodeLabels", "x(key=value)" };
    assertTrue(0 != rmAdminCLI.run(args));

    args = new String[] { "-addToClusterNodeLabels", "x(exclusive=)" };
    assertTrue(0 != rmAdminCLI.run(args));

    args =
        new String[] { "-addToClusterNodeLabels",
            "w,x(exclusive=true), y(exclusive=false),z()",
            "-directlyAccessNodeLabelStore" };
    assertTrue(0 == rmAdminCLI.run(args));

    assertTrue(dummyNodeLabelsManager.isExclusiveNodeLabel("w"));
    assertTrue(dummyNodeLabelsManager.isExclusiveNodeLabel("x"));
    assertFalse(dummyNodeLabelsManager.isExclusiveNodeLabel("y"));
    assertTrue(dummyNodeLabelsManager.isExclusiveNodeLabel("z"));

    args =
        new String[] { "-addToClusterNodeLabels",
            "a (exclusive= true) , b( exclusive =false),c  ",
            "-directlyAccessNodeLabelStore" };
    assertTrue(0 == rmAdminCLI.run(args));

    assertTrue(dummyNodeLabelsManager.isExclusiveNodeLabel("a"));
    assertFalse(dummyNodeLabelsManager.isExclusiveNodeLabel("b"));
    assertTrue(dummyNodeLabelsManager.isExclusiveNodeLabel("c"));
  }

  @Test
  public void testRemoveFromClusterNodeLabels() throws Exception {
    dummyNodeLabelsManager.addToCluserNodeLabelsWithDefaultExclusivity(ImmutableSet.of("x", "y"));
    String[] args =
        { "-removeFromClusterNodeLabels", "x,,y",
            "-directlyAccessNodeLabelStore" };
    assertEquals(0, rmAdminCLI.run(args));
    assertTrue(dummyNodeLabelsManager.getClusterNodeLabelNames().isEmpty());
    
    args = new String[] { "-removeFromClusterNodeLabels" };
  public void testReplaceLabelsOnNode() throws Exception {
    dummyNodeLabelsManager
        .addToCluserNodeLabelsWithDefaultExclusivity(ImmutableSet.of("x", "y", "Y"));
    String[] args =
        { "-replaceLabelsOnNode",
            "node1:8000,x node2:8000=y node3,x node4=Y",
  @Test
  public void testReplaceMultipleLabelsOnSingleNode() throws Exception {
    dummyNodeLabelsManager.addToCluserNodeLabelsWithDefaultExclusivity(ImmutableSet.of("x", "y"));
    String[] args =
        { "-replaceLabelsOnNode", "node1,x,y",
            "-directlyAccessNodeLabelStore" };

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/main/java/org/apache/hadoop/yarn/api/protocolrecords/impl/pb/GetClusterNodeLabelsResponsePBImpl.java

package org.apache.hadoop.yarn.api.protocolrecords.impl.pb;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodeLabelsResponse;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.api.records.impl.pb.NodeLabelPBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.NodeLabelProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetClusterNodeLabelsResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetClusterNodeLabelsResponseProtoOrBuilder;

public class GetClusterNodeLabelsResponsePBImpl extends
    GetClusterNodeLabelsResponse {
  GetClusterNodeLabelsResponseProto proto = GetClusterNodeLabelsResponseProto
      .getDefaultInstance();
  GetClusterNodeLabelsResponseProto.Builder builder = null;
  private List<NodeLabel> updatedNodeLabels;
  boolean viaProto = false;

  public GetClusterNodeLabelsResponsePBImpl() {
    builder = GetClusterNodeLabelsResponseProto.newBuilder();
  }

  public GetClusterNodeLabelsResponsePBImpl(
    viaProto = true;
  }

  public GetClusterNodeLabelsResponseProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToProto() {
    viaProto = true;
  }

  private void mergeLocalToBuilder() {
    if (this.updatedNodeLabels != null) {
      addNodeLabelsToProto();
    }
  }

  private void addNodeLabelsToProto() {
    maybeInitBuilder();
    builder.clearNodeLabels();
    List<NodeLabelProto> protoList = new ArrayList<NodeLabelProto>();
    for (NodeLabel r : this.updatedNodeLabels) {
      protoList.add(convertToProtoFormat(r));
    }
    builder.addAllNodeLabels(protoList);
  }

  @Override
  public boolean equals(Object other) {
    if (other == null)
      return false;
    if (other.getClass().isAssignableFrom(this.getClass())) {
      return this.getProto().equals(this.getClass().cast(other).getProto());
    }
    return false;
  }

  @Override
    return 0;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = GetClusterNodeLabelsResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public void setNodeLabels(List<NodeLabel> updatedNodeLabels) {
    maybeInitBuilder();
    this.updatedNodeLabels = new ArrayList<>();
    if (updatedNodeLabels == null) {
      builder.clearNodeLabels();
      return;
    }
    this.updatedNodeLabels.addAll(updatedNodeLabels);
  }

  private void initLocalNodeLabels() {
    GetClusterNodeLabelsResponseProtoOrBuilder p = viaProto ? proto : builder;
    List<NodeLabelProto> attributesProtoList = p.getNodeLabelsList();
    this.updatedNodeLabels = new ArrayList<NodeLabel>();
    for (NodeLabelProto r : attributesProtoList) {
      this.updatedNodeLabels.add(convertFromProtoFormat(r));
    }
  }

  @Override
  public List<NodeLabel> getNodeLabels() {
    if (this.updatedNodeLabels != null) {
      return this.updatedNodeLabels;
    }
    initLocalNodeLabels();
    return this.updatedNodeLabels;
  }

  private NodeLabel convertFromProtoFormat(NodeLabelProto p) {
    return new NodeLabelPBImpl(p);
  }

  private NodeLabelProto convertToProtoFormat(NodeLabel t) {
    return ((NodeLabelPBImpl) t).getProto();
  }

  @Override
  public String toString() {
    return getProto().toString();
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/main/java/org/apache/hadoop/yarn/api/records/impl/pb/NodeLabelPBImpl.java
  }

  @Override
  public String getName() {
    NodeLabelProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasName()) {
      return null;
    }
    return (p.getName());
  }

  @Override
  public void setName(String name) {
    maybeInitBuilder();
    if (name == null) {
      builder.clearName();
      return;
    }
    builder.setName(name);
  }

  @Override
  public boolean isExclusive() {
    NodeLabelProtoOrBuilder p = viaProto ? proto : builder;
    return p.getIsExclusive();
  }

  @Override
  public void setExclusivity(boolean isExclusive) {
    maybeInitBuilder();
    builder.setIsExclusive(isExclusive);
  }

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/main/java/org/apache/hadoop/yarn/nodelabels/CommonNodeLabelsManager.java
package org.apache.hadoop.yarn.nodelabels;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.nodelabels.event.NodeLabelsStoreEvent;
import org.apache.hadoop.yarn.nodelabels.event.NodeLabelsStoreEventType;
import org.apache.hadoop.yarn.nodelabels.event.RemoveClusterNodeLabels;
import org.apache.hadoop.yarn.nodelabels.event.StoreNewClusterNodeLabels;
import org.apache.hadoop.yarn.nodelabels.event.UpdateNodeToLabelsMappingsEvent;
import org.apache.hadoop.yarn.util.resource.Resources;

        store.updateNodeToLabelsMappings(updateNodeToLabelsMappingsEvent
            .getNodeToLabels());
        break;
      }
    } catch (IOException e) {
      LOG.error("Failed to store label modification to storage");
    }
  }

  @SuppressWarnings("unchecked")
  public void addToCluserNodeLabels(Collection<NodeLabel> labels)
      throws IOException {
    if (!nodeLabelsEnabled) {
      LOG.error(NODE_LABELS_NOT_ENABLED_ERR);
      throw new IOException(NODE_LABELS_NOT_ENABLED_ERR);
    if (null == labels || labels.isEmpty()) {
      return;
    }
    List<NodeLabel> newLabels = new ArrayList<NodeLabel>();
    normalizeNodeLabels(labels);

    for (NodeLabel label : labels) {
      checkAndThrowLabelName(label.getName());
    }

    for (NodeLabel label : labels) {
      if (this.labelCollections.get(label.getName()) == null) {
        this.labelCollections.put(label.getName(), new RMNodeLabel(label));
        newLabels.add(label);
      }
    }
    LOG.info("Add labels: [" + StringUtils.join(labels.iterator(), ",") + "]");
  }

  @VisibleForTesting
  public void addToCluserNodeLabelsWithDefaultExclusivity(Set<String> labels)
      throws IOException {
    Set<NodeLabel> nodeLabels = new HashSet<NodeLabel>();
    for (String label : labels) {
      nodeLabels.add(NodeLabel.newInstance(label));
    }
    addToCluserNodeLabels(nodeLabels);
  }
  
  protected void checkAddLabelsToNode(
      Map<NodeId, Set<String>> addedLabelsToNode) throws IOException {
    if (null == addedLabelsToNode || addedLabelsToNode.isEmpty()) {
  public Set<String> getClusterNodeLabelNames() {
    try {
      readLock.lock();
      Set<String> labels = new HashSet<String>(labelCollections.keySet());
    }
  }
  
  public List<NodeLabel> getClusterNodeLabels() {
    try {
      readLock.lock();
      List<NodeLabel> nodeLabels = new ArrayList<>();
      for (RMNodeLabel label : labelCollections.values()) {
        nodeLabels.add(NodeLabel.newInstance(label.getLabelName(),
            label.getIsExclusive()));
      }
      return nodeLabels;
    } finally {
      readLock.unlock();
    }
  }

    return newLabels;
  }
  
  private void normalizeNodeLabels(Collection<NodeLabel> labels) {
    for (NodeLabel label : labels) {
      label.setName(normalizeLabel(label.getName()));
    }
  }

  protected Node getNMInNodeSet(NodeId nodeId) {
    return getNMInNodeSet(nodeId, nodeCollections);
  }

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/main/java/org/apache/hadoop/yarn/nodelabels/FileSystemNodeLabelsStore.java
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.AddToClusterNodeLabelsRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.RemoveFromClusterNodeLabelsRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.ReplaceLabelsOnNodeRequestProto;
import org.apache.hadoop.yarn.server.api.protocolrecords.AddToClusterNodeLabelsRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RemoveFromClusterNodeLabelsRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.ReplaceLabelsOnNodeRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.AddToClusterNodeLabelsRequestPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.RemoveFromClusterNodeLabelsRequestPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.ReplaceLabelsOnNodeRequestPBImpl;

import com.google.common.collect.Sets;

  protected static final String EDITLOG_FILENAME = "nodelabel.editlog";
  
  protected enum SerializedLogType {
    ADD_LABELS, NODE_TO_LABELS, REMOVE_LABELS
  }

  Path fsWorkingPath;
  }

  @Override
  public void storeNewClusterNodeLabels(List<NodeLabel> labels)
      throws IOException {
    ensureAppendEditlogFile();
    editlogOs.writeInt(SerializedLogType.ADD_LABELS.ordinal());
    ((AddToClusterNodeLabelsRequestPBImpl) AddToClusterNodeLabelsRequest
        .newInstance(labels)).getProto().writeDelimitedTo(editlogOs);
    ensureCloseEditlogFile();
  }

    ensureCloseEditlogFile();
  }

  @Override
  public void recover() throws YarnException, IOException {
    }

    if (null != is) {
      List<NodeLabel> labels =
          new AddToClusterNodeLabelsRequestPBImpl(
              AddToClusterNodeLabelsRequestProto.parseDelimitedFrom(is)).getNodeLabels();
      Map<NodeId, Set<String>> nodeToLabels =
          
          switch (type) {
          case ADD_LABELS: {
            List<NodeLabel> labels =
                new AddToClusterNodeLabelsRequestPBImpl(
                    AddToClusterNodeLabelsRequestProto.parseDelimitedFrom(is))
                    .getNodeLabels();
            mgr.addToCluserNodeLabels(labels);
            break;
          }
          case REMOVE_LABELS: {
            mgr.replaceLabelsOnNode(map);
            break;
          }
          }
        } catch (EOFException e) {

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/main/java/org/apache/hadoop/yarn/nodelabels/NodeLabelsStore.java
  public abstract void storeNewClusterNodeLabels(List<NodeLabel> label)
      throws IOException;

  public abstract void removeClusterNodeLabels(Collection<String> labels)
      throws IOException;


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/main/java/org/apache/hadoop/yarn/nodelabels/RMNodeLabel.java

import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.resource.Resources;

public class RMNodeLabel implements Comparable<RMNodeLabel> {
  private int numActiveNMs;
  private String labelName;
  private Set<NodeId> nodeIds;
  private boolean exclusive;

  public RMNodeLabel(NodeLabel nodeLabel) {
    this(nodeLabel.getName(), Resource.newInstance(0, 0), 0,
        nodeLabel.isExclusive());
  }

  public RMNodeLabel(String labelName) {
    this(labelName, Resource.newInstance(0, 0), 0,
        NodeLabel.DEFAULT_NODE_LABEL_EXCLUSIVITY);
  }
  
  protected RMNodeLabel(String labelName, Resource res, int activeNMs,
      boolean exclusive) {
    this.labelName = labelName;
    this.resource = res;
    this.numActiveNMs = activeNMs;
    this.nodeIds = new HashSet<NodeId>();
    this.exclusive = exclusive;
  }

  public void addNodeId(NodeId node) {
  }
  
  public RMNodeLabel getCopy() {
    return new RMNodeLabel(labelName, resource, numActiveNMs, exclusive);
  }
  
  @Override

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/main/java/org/apache/hadoop/yarn/nodelabels/event/NodeLabelsStoreEventType.java
public enum NodeLabelsStoreEventType {
  REMOVE_LABELS,
  ADD_LABELS,
  STORE_NODE_TO_LABELS
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/main/java/org/apache/hadoop/yarn/nodelabels/event/StoreNewClusterNodeLabels.java

package org.apache.hadoop.yarn.nodelabels.event;

import java.util.List;

import org.apache.hadoop.yarn.api.records.NodeLabel;

public class StoreNewClusterNodeLabels extends NodeLabelsStoreEvent {
  private List<NodeLabel> labels;
  
  public StoreNewClusterNodeLabels(List<NodeLabel> labels) {
    super(NodeLabelsStoreEventType.ADD_LABELS);
    this.labels = labels;
  }
  
  public List<NodeLabel> getLabels() {
    return labels;
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/main/java/org/apache/hadoop/yarn/server/api/impl/pb/client/ResourceManagerAdministrationProtocolPBClientImpl.java
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.RefreshUserToGroupsMappingsRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.RemoveFromClusterNodeLabelsRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.ReplaceLabelsOnNodeRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.UpdateNodeResourceRequestProto;
import org.apache.hadoop.yarn.server.api.ResourceManagerAdministrationProtocol;
import org.apache.hadoop.yarn.server.api.ResourceManagerAdministrationProtocolPB;
import org.apache.hadoop.yarn.server.api.protocolrecords.RemoveFromClusterNodeLabelsResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.ReplaceLabelsOnNodeRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.ReplaceLabelsOnNodeResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.UpdateNodeResourceRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.UpdateNodeResourceResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.AddToClusterNodeLabelsRequestPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.RemoveFromClusterNodeLabelsResponsePBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.ReplaceLabelsOnNodeRequestPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.ReplaceLabelsOnNodeResponsePBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.UpdateNodeResourceRequestPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.UpdateNodeResourceResponsePBImpl;

    }
  }

  @Override
  public CheckForDecommissioningNodesResponse checkForDecommissioningNodes(
      CheckForDecommissioningNodesRequest checkForDecommissioningNodesRequest)

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/main/java/org/apache/hadoop/yarn/server/api/impl/pb/service/ResourceManagerAdministrationProtocolPBServiceImpl.java
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.RemoveFromClusterNodeLabelsResponseProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.ReplaceLabelsOnNodeRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.ReplaceLabelsOnNodeResponseProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.UpdateNodeResourceRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.UpdateNodeResourceResponseProto;
import org.apache.hadoop.yarn.server.api.ResourceManagerAdministrationProtocol;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshUserToGroupsMappingsResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RemoveFromClusterNodeLabelsResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.ReplaceLabelsOnNodeResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.UpdateNodeResourceResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.AddToClusterNodeLabelsRequestPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.AddToClusterNodeLabelsResponsePBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.RemoveFromClusterNodeLabelsResponsePBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.ReplaceLabelsOnNodeRequestPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.ReplaceLabelsOnNodeResponsePBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.UpdateNodeResourceRequestPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.UpdateNodeResourceResponsePBImpl;

    }
  }

  @Override
  public CheckForDecommissioningNodesResponseProto checkForDecommissioningNodes(
      RpcController controller, CheckForDecommissioningNodesRequestProto proto)

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/main/java/org/apache/hadoop/yarn/server/api/protocolrecords/impl/pb/AddToClusterNodeLabelsRequestPBImpl.java

package org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.api.records.impl.pb.NodeLabelPBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.NodeLabelProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.AddToClusterNodeLabelsRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.AddToClusterNodeLabelsRequestProtoOrBuilder;
import org.apache.hadoop.yarn.server.api.protocolrecords.AddToClusterNodeLabelsRequest;

public class AddToClusterNodeLabelsRequestPBImpl extends
    AddToClusterNodeLabelsRequest {
  AddToClusterNodeLabelsRequestProto proto = AddToClusterNodeLabelsRequestProto
      .getDefaultInstance();
  AddToClusterNodeLabelsRequestProto.Builder builder = null;
  private List<NodeLabel> updatedNodeLabels;
  boolean viaProto = false;

  public AddToClusterNodeLabelsRequestPBImpl() {
    builder = AddToClusterNodeLabelsRequestProto.newBuilder();
  }

  public AddToClusterNodeLabelsRequestPBImpl(
    viaProto = true;
  }

  public AddToClusterNodeLabelsRequestProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToProto() {
    viaProto = true;
  }

  private void mergeLocalToBuilder() {
    if (this.updatedNodeLabels != null) {
      addNodeLabelsToProto();
    }
  }

  private void addNodeLabelsToProto() {
    maybeInitBuilder();
    builder.clearNodeLabels();
    List<NodeLabelProto> protoList = new ArrayList<NodeLabelProto>();
    for (NodeLabel r : this.updatedNodeLabels) {
      protoList.add(convertToProtoFormat(r));
    }
    builder.addAllNodeLabels(protoList);
  }

  @Override
  public boolean equals(Object other) {
    if (other == null)
      return false;
    if (other.getClass().isAssignableFrom(this.getClass())) {
      return this.getProto().equals(this.getClass().cast(other).getProto());
    }
    return false;
  }

  @Override
    return 0;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = AddToClusterNodeLabelsRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public void setNodeLabels(List<NodeLabel> updatedNodeLabels) {
    maybeInitBuilder();
    this.updatedNodeLabels = new ArrayList<>();
    if (updatedNodeLabels == null) {
      builder.clearNodeLabels();
      return;
    }
    this.updatedNodeLabels.addAll(updatedNodeLabels);
  }

  private void initLocalNodeLabels() {
    AddToClusterNodeLabelsRequestProtoOrBuilder p = viaProto ? proto : builder;
    List<NodeLabelProto> attributesProtoList = p.getNodeLabelsList();
    this.updatedNodeLabels = new ArrayList<NodeLabel>();
    for (NodeLabelProto r : attributesProtoList) {
      this.updatedNodeLabels.add(convertFromProtoFormat(r));
    }
  }

  @Override
  public List<NodeLabel> getNodeLabels() {
    if (this.updatedNodeLabels != null) {
      return this.updatedNodeLabels;
    }
    initLocalNodeLabels();
    return this.updatedNodeLabels;
  }

  private NodeLabel convertFromProtoFormat(NodeLabelProto p) {
    return new NodeLabelPBImpl(p);
  }

  private NodeLabelProto convertToProtoFormat(NodeLabel t) {
    return ((NodeLabelPBImpl) t).getProto();
  }

  @Override
  public String toString() {
    return getProto().toString();
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/test/java/org/apache/hadoop/yarn/api/TestPBImplRecords.java
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.RemoveFromClusterNodeLabelsResponseProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.ReplaceLabelsOnNodeRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.ReplaceLabelsOnNodeResponseProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.UpdateNodeResourceRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.UpdateNodeResourceResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.AllocateRequestProto;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.RemoveFromClusterNodeLabelsResponsePBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.ReplaceLabelsOnNodeRequestPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.ReplaceLabelsOnNodeResponsePBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.UpdateNodeResourceRequestPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.UpdateNodeResourceResponsePBImpl;
import org.apache.hadoop.yarn.util.resource.Resources;
        NodeLabelProto.class);
  }
  
  @Test
  public void testCheckForDecommissioningNodesRequestPBImpl() throws Exception {
    validatePBImplRecord(CheckForDecommissioningNodesRequestPBImpl.class,

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/test/java/org/apache/hadoop/yarn/nodelabels/DummyCommonNodeLabelsManager.java

public class DummyCommonNodeLabelsManager extends CommonNodeLabelsManager {
  Map<NodeId, Set<String>> lastNodeToLabels = null;
  Collection<NodeLabel> lastAddedlabels = null;
  Collection<String> lastRemovedlabels = null;

  @Override
  public void initNodeLabelStore(Configuration conf) {
      }

      @Override
      public void storeNewClusterNodeLabels(List<NodeLabel> label) throws IOException {
        lastAddedlabels = label;
      }

      @Override
      public void close() throws IOException {

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/test/java/org/apache/hadoop/yarn/nodelabels/NodeLabelTestBase.java
    Assert.assertTrue(s1.containsAll(s2));
  }

  @SuppressWarnings("unchecked")
  public static <E> Set<E> toSet(E... elements) {
    Set<E> set = Sets.newHashSet(elements);
    return set;

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/test/java/org/apache/hadoop/yarn/nodelabels/TestCommonNodeLabelsManager.java

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
  @Test(timeout = 5000)
  public void testAddRemovelabel() throws Exception {
    mgr.addToCluserNodeLabelsWithDefaultExclusivity(ImmutableSet.of("hello"));
    verifyNodeLabelAdded(Sets.newHashSet("hello"), mgr.lastAddedlabels);

    mgr.addToCluserNodeLabelsWithDefaultExclusivity(ImmutableSet.of("world"));
    mgr.addToCluserNodeLabelsWithDefaultExclusivity(toSet("hello1", "world1"));
    verifyNodeLabelAdded(Sets.newHashSet("hello1", "world1"), mgr.lastAddedlabels);

    Assert.assertTrue(mgr.getClusterNodeLabelNames().containsAll(
        Sets.newHashSet("hello", "world", "hello1", "world1")));


    mgr.removeFromClusterNodeLabels(Arrays.asList("hello"));
    assertCollectionEquals(Sets.newHashSet("hello"), mgr.lastRemovedlabels);
    Assert.assertTrue(mgr.getClusterNodeLabelNames().containsAll(
        Arrays.asList("world", "hello1", "world1")));

    mgr.removeFromClusterNodeLabels(Arrays
        .asList("hello1", "world1", "world"));
    Assert.assertTrue(mgr.lastRemovedlabels.containsAll(Sets.newHashSet(
        "hello1", "world1", "world")));
    Assert.assertTrue(mgr.getClusterNodeLabelNames().isEmpty());
  }

  @Test(timeout = 5000)
  public void testAddlabelWithCase() throws Exception {
    mgr.addToCluserNodeLabelsWithDefaultExclusivity(ImmutableSet.of("HeLlO"));
    verifyNodeLabelAdded(Sets.newHashSet("HeLlO"), mgr.lastAddedlabels);
    Assert.assertFalse(mgr.getClusterNodeLabelNames().containsAll(
        Arrays.asList("hello")));
  }

  @Test(timeout = 5000)
  public void testAddlabelWithExclusivity() throws Exception {
    mgr.addToCluserNodeLabels(Arrays.asList(NodeLabel.newInstance("a", false), NodeLabel.newInstance("b", true)));
    Assert.assertFalse(mgr.isExclusiveNodeLabel("a"));
    Assert.assertTrue(mgr.isExclusiveNodeLabel("b"));
  }

  @Test(timeout = 5000)
    try {
      Set<String> set = new HashSet<String>();
      set.add(null);
      mgr.addToCluserNodeLabelsWithDefaultExclusivity(set);
    } catch (IOException e) {
      caught = true;
    }

    caught = false;
    try {
      mgr.addToCluserNodeLabelsWithDefaultExclusivity(ImmutableSet.of(CommonNodeLabelsManager.NO_LABEL));
    } catch (IOException e) {
      caught = true;
    }

    caught = false;
    try {
      mgr.addToCluserNodeLabelsWithDefaultExclusivity(ImmutableSet.of("-?"));
    } catch (IOException e) {
      caught = true;
    }

    caught = false;
    try {
      mgr.addToCluserNodeLabelsWithDefaultExclusivity(ImmutableSet.of(StringUtils.repeat("c", 257)));
    } catch (IOException e) {
      caught = true;
    }

    caught = false;
    try {
      mgr.addToCluserNodeLabelsWithDefaultExclusivity(ImmutableSet.of("-aaabbb"));
    } catch (IOException e) {
      caught = true;
    }

    caught = false;
    try {
      mgr.addToCluserNodeLabelsWithDefaultExclusivity(ImmutableSet.of("_aaabbb"));
    } catch (IOException e) {
      caught = true;
    }
    
    caught = false;
    try {
      mgr.addToCluserNodeLabelsWithDefaultExclusivity(ImmutableSet.of("a^aabbb"));
    } catch (IOException e) {
      caught = true;
    }
    
    caught = false;
    try {
      mgr.addToCluserNodeLabelsWithDefaultExclusivity(ImmutableSet.of("aa[a]bbb"));
    } catch (IOException e) {
      caught = true;
    }
    Assert.assertTrue("trying to add a empty node but succeeded", caught);

    mgr.addToCluserNodeLabelsWithDefaultExclusivity(toSet("p1", "p2", "p3"));
    mgr.replaceLabelsOnNode(ImmutableMap.of(toNodeId("n1"), toSet("p1")));
    mgr.replaceLabelsOnNode(ImmutableMap.of(toNodeId("n1"), toSet("p2")));
    mgr.replaceLabelsOnNode(ImmutableMap.of(toNodeId("n2"), toSet("p3")));

  @Test(timeout = 5000)
  public void testRemovelabelWithNodes() throws Exception {
    mgr.addToCluserNodeLabelsWithDefaultExclusivity(toSet("p1", "p2", "p3"));
    mgr.replaceLabelsOnNode(ImmutableMap.of(toNodeId("n1"), toSet("p1")));
    mgr.replaceLabelsOnNode(ImmutableMap.of(toNodeId("n2"), toSet("p2")));
    mgr.replaceLabelsOnNode(ImmutableMap.of(toNodeId("n3"), toSet("p3")));

    mgr.removeFromClusterNodeLabels(ImmutableSet.of("p2", "p3"));
    Assert.assertTrue(mgr.getNodeLabels().isEmpty());
    Assert.assertTrue(mgr.getClusterNodeLabelNames().isEmpty());
    assertCollectionEquals(mgr.lastRemovedlabels, Arrays.asList("p2", "p3"));
  }
  
  @Test(timeout = 5000) 
  public void testTrimLabelsWhenAddRemoveNodeLabels() throws IOException {
    mgr.addToCluserNodeLabelsWithDefaultExclusivity(toSet(" p1"));
    assertCollectionEquals(mgr.getClusterNodeLabelNames(), toSet("p1"));
    mgr.removeFromClusterNodeLabels(toSet("p1 "));
    Assert.assertTrue(mgr.getClusterNodeLabelNames().isEmpty());
  }
  
  @Test(timeout = 5000) 
  public void testTrimLabelsWhenModifyLabelsOnNodes() throws IOException {
    mgr.addToCluserNodeLabelsWithDefaultExclusivity(toSet(" p1", "p2"));
    mgr.addLabelsToNode(ImmutableMap.of(toNodeId("n1"), toSet("p1 ")));
    assertMapEquals(
        mgr.getNodeLabels(),
  @Test(timeout = 5000)
  public void testReplaceLabelsOnHostsShouldUpdateNodesBelongTo()
      throws IOException {
    mgr.addToCluserNodeLabelsWithDefaultExclusivity(toSet("p1", "p2", "p3"));
    mgr.addLabelsToNode(ImmutableMap.of(toNodeId("n1"), toSet("p1")));
    assertMapEquals(
        mgr.getNodeLabels(),
    
    try {
      mgr.addToCluserNodeLabelsWithDefaultExclusivity(ImmutableSet.of("x"));
    } catch (IOException e) {
      assertNodeLabelsDisabledErrorMessage(e);
      caught = true;
  @Test(timeout = 5000)
  public void testLabelsToNodes()
      throws IOException {
    mgr.addToCluserNodeLabelsWithDefaultExclusivity(toSet("p1", "p2", "p3"));
    mgr.addLabelsToNode(ImmutableMap.of(toNodeId("n1"), toSet("p1")));
    Map<String, Set<NodeId>> labelsToNodes = mgr.getLabelsToNodes();
    assertLabelsToNodesEquals(
  @Test(timeout = 5000)
  public void testLabelsToNodesForSelectedLabels()
      throws IOException {
    mgr.addToCluserNodeLabelsWithDefaultExclusivity(toSet("p1", "p2", "p3"));
    mgr.addLabelsToNode(
        ImmutableMap.of(
        toNodeId("n1:1"), toSet("p1"),
    boolean failed = false;
    mgr.addToCluserNodeLabelsWithDefaultExclusivity(toSet("p1", "p2", "p3"));
    try {
      mgr.replaceLabelsOnNode(ImmutableMap.of(toNodeId("n1"), toSet("p1", "p2")));
    } catch (IOException e) {
        failed);
  }

  private void verifyNodeLabelAdded(Set<String> expectedAddedLabelNames,
      Collection<NodeLabel> addedNodeLabels) {
    Assert.assertEquals(expectedAddedLabelNames.size(), addedNodeLabels.size());
    for (NodeLabel label : addedNodeLabels) {
      Assert.assertTrue(expectedAddedLabelNames.contains(label.getName()));
    }
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/test/java/org/apache/hadoop/yarn/nodelabels/TestFileSystemNodeLabelsStore.java
  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Test(timeout = 10000)
  public void testRecoverWithMirror() throws Exception {
    mgr.addToCluserNodeLabelsWithDefaultExclusivity(toSet("p1", "p2", "p3"));
    mgr.addToCluserNodeLabelsWithDefaultExclusivity(toSet("p4"));
    mgr.addToCluserNodeLabelsWithDefaultExclusivity(toSet("p5", "p6"));
    mgr.replaceLabelsOnNode((Map) ImmutableMap.of(toNodeId("n1"), toSet("p1"),
        toNodeId("n2"), toSet("p2")));
    mgr.replaceLabelsOnNode((Map) ImmutableMap.of(toNodeId("n3"), toSet("p3"),
    mgr.init(conf);

    Assert.assertEquals(3, mgr.getClusterNodeLabelNames().size());
    Assert.assertTrue(mgr.getClusterNodeLabelNames().containsAll(
        Arrays.asList("p2", "p4", "p6")));

    assertMapContains(mgr.getNodeLabels(), ImmutableMap.of(toNodeId("n2"),
    mgr.init(conf);

    Assert.assertEquals(3, mgr.getClusterNodeLabelNames().size());
    Assert.assertTrue(mgr.getClusterNodeLabelNames().containsAll(
        Arrays.asList("p2", "p4", "p6")));

    assertMapContains(mgr.getNodeLabels(), ImmutableMap.of(toNodeId("n2"),
  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Test(timeout = 10000)
  public void testEditlogRecover() throws Exception {
    mgr.addToCluserNodeLabelsWithDefaultExclusivity(toSet("p1", "p2", "p3"));
    mgr.addToCluserNodeLabelsWithDefaultExclusivity(toSet("p4"));
    mgr.addToCluserNodeLabelsWithDefaultExclusivity(toSet("p5", "p6"));
    mgr.replaceLabelsOnNode(ImmutableMap.of(toNodeId("n1"), toSet("p1"),
        toNodeId("n2"), toSet("p2")));
    mgr.replaceLabelsOnNode((Map) ImmutableMap.of(toNodeId("n3"), toSet("p3"),
    mgr.init(conf);

    Assert.assertEquals(3, mgr.getClusterNodeLabelNames().size());
    Assert.assertTrue(mgr.getClusterNodeLabelNames().containsAll(
        Arrays.asList("p2", "p4", "p6")));

    assertMapContains(mgr.getNodeLabels(), ImmutableMap.of(toNodeId("n2"),
  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Test (timeout = 10000)
  public void testSerilizationAfterRecovery() throws Exception {
    mgr.addToCluserNodeLabels(Arrays.asList(NodeLabel.newInstance("p1", true),
        NodeLabel.newInstance("p2", false), NodeLabel.newInstance("p3", true),
        NodeLabel.newInstance("p4", true), NodeLabel.newInstance("p5", true),
        NodeLabel.newInstance("p6", false)));

    mgr.replaceLabelsOnNode(ImmutableMap.of(toNodeId("n1"), toSet("p1"),
        toNodeId("n2"), toSet("p2")));
    mgr.replaceLabelsOnNode((Map) ImmutableMap.of(toNodeId("n3"), toSet("p3"),

    mgr.stop();

    mgr.start();

    Assert.assertEquals(3, mgr.getClusterNodeLabelNames().size());
    Assert.assertTrue(mgr.getClusterNodeLabelNames().containsAll(
        Arrays.asList("p2", "p4", "p6")));

    assertMapContains(mgr.getNodeLabels(), ImmutableMap.of(toNodeId("n2"),
    mgr = new MockNodeLabelManager();
    mgr.init(conf);
    mgr.start();
    mgr.addToCluserNodeLabelsWithDefaultExclusivity(toSet("p7", "p8"));
    mgr.stop();
    
    mgr = new MockNodeLabelManager();
    mgr.init(conf);
    mgr.start();
    mgr.addToCluserNodeLabelsWithDefaultExclusivity(toSet("p9"));
    mgr.stop();
    
    mgr.start();

    Assert.assertEquals(6, mgr.getClusterNodeLabelNames().size());
    Assert.assertTrue(mgr.getClusterNodeLabelNames().containsAll(
        Arrays.asList("p2", "p4", "p6", "p7", "p8", "p9")));
    mgr.stop();
  }

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/AdminService.java
import org.apache.hadoop.yarn.server.api.protocolrecords.RemoveFromClusterNodeLabelsResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.ReplaceLabelsOnNodeRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.ReplaceLabelsOnNodeResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.UpdateNodeResourceRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.UpdateNodeResourceResponse;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationSystem;
    }
  }

  private void checkRMStatus(String user, String argName, String msg)
      throws StandbyException {
    if (!isRMActive()) {

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/CSQueueUtils.java
      accessibleLabels.addAll(labels);
    }
    if (accessibleLabels.contains(CommonNodeLabelsManager.ANY)) {
      accessibleLabels.addAll(mgr.getClusterNodeLabelNames());
    }
    accessibleLabels.add(CommonNodeLabelsManager.NO_LABEL);
    

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/ParentQueue.java
      		" for children of queue " + queueName);
    }
    for (String nodeLabel : labelManager.getClusterNodeLabelNames()) {
      float capacityByLabel = queueCapacities.getCapacity(nodeLabel);
      float sum = 0;

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/webapp/RMWebServices.java

    NodeLabelsInfo ret = 
      new NodeLabelsInfo(rm.getRMContext().getNodeLabelManager()
        .getClusterNodeLabelNames());

    return ret;
  }
    }
    
    rm.getRMContext().getNodeLabelManager()
        .addToCluserNodeLabelsWithDefaultExclusivity(new HashSet<String>(
          newNodeLabels.getNodeLabels()));
            
    return Response.status(Status.OK).build();

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/TestClientRMService.java
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.QueueACL;
    };
    rm.start();
    RMNodeLabelsManager labelsMgr = rm.getRMContext().getNodeLabelManager();
    labelsMgr.addToCluserNodeLabelsWithDefaultExclusivity(ImmutableSet.of("x", "y"));

    MockNM node = rm.registerNode("host1:1234", 1024);
    };
    rm.start();
    RMNodeLabelsManager labelsMgr = rm.getRMContext().getNodeLabelManager();
    labelsMgr.addToCluserNodeLabelsWithDefaultExclusivity(ImmutableSet.of("x", "y"));

    Map<NodeId, Set<String>> map = new HashMap<NodeId, Set<String>>();
    map.put(NodeId.newInstance("host1", 0), ImmutableSet.of("x"));
    GetClusterNodeLabelsResponse response =
        client.getClusterNodeLabels(GetClusterNodeLabelsRequest.newInstance());
    Assert.assertTrue(response.getNodeLabels().containsAll(
        Arrays.asList(NodeLabel.newInstance("x"), NodeLabel.newInstance("y"))));

    GetNodesToLabelsResponse response1 =
    };
    rm.start();
    RMNodeLabelsManager labelsMgr = rm.getRMContext().getNodeLabelManager();
    labelsMgr.addToCluserNodeLabelsWithDefaultExclusivity(ImmutableSet.of("x", "y", "z"));

    Map<NodeId, Set<String>> map = new HashMap<NodeId, Set<String>>();
    map.put(NodeId.newInstance("host1", 0), ImmutableSet.of("x"));
    GetClusterNodeLabelsResponse response =
        client.getClusterNodeLabels(GetClusterNodeLabelsRequest.newInstance());
    Assert.assertTrue(response.getNodeLabels().containsAll(
        Arrays.asList(NodeLabel.newInstance("x"), NodeLabel.newInstance("y"),
            NodeLabel.newInstance("z"))));

    GetLabelsToNodesResponse response1 =

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/TestRMRestart.java
    clusterNodeLabels.add("y");
    clusterNodeLabels.add("z");
    nodeLabelManager.addToCluserNodeLabelsWithDefaultExclusivity(clusterNodeLabels);

    NodeId n1 = NodeId.newInstance("h1", 0);
    }

    Assert.assertEquals(clusterNodeLabels.size(), nodeLabelManager
        .getClusterNodeLabelNames().size());

    Map<NodeId, Set<String>> nodeLabels = nodeLabelManager.getNodeLabels();
    Assert.assertEquals(1, nodeLabelManager.getNodeLabels().size());

    nodeLabelManager = rm2.getRMContext().getNodeLabelManager();
    Assert.assertEquals(clusterNodeLabels.size(), nodeLabelManager
        .getClusterNodeLabelNames().size());

    nodeLabels = nodeLabelManager.getNodeLabels();
    Assert.assertEquals(1, nodeLabelManager.getNodeLabels().size());

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/TestResourceTrackerService.java
    rm.start();

    try {
      nodeLabelsMgr.addToCluserNodeLabelsWithDefaultExclusivity(toSet("A", "B", "C"));
    } catch (IOException e) {
      Assert.fail("Caught Exception while intializing");
      e.printStackTrace();
    rm.start();

    try {
      nodeLabelsMgr.addToCluserNodeLabelsWithDefaultExclusivity(toSet("X", "Y", "Z"));
    } catch (IOException e) {
      Assert.fail("Caught Exception while intializing");
      e.printStackTrace();
    rm.start();

    try {
      nodeLabelsMgr.addToCluserNodeLabelsWithDefaultExclusivity(toSet("X", "Y", "Z"));
    } catch (IOException e) {
      Assert.fail("Caught Exception while intializing");
      e.printStackTrace();
    };
    rm.start();
    try {
      nodeLabelsMgr.addToCluserNodeLabelsWithDefaultExclusivity(toSet("A", "B", "C"));
    } catch (IOException e) {
      Assert.fail("Caught Exception while intializing");
      e.printStackTrace();
    rm.start();
    try {
      nodeLabelsMgr.addToCluserNodeLabelsWithDefaultExclusivity(toSet("A", "B", "C"));
    } catch (IOException e) {
      Assert.fail("Caught Exception while intializing");
      e.printStackTrace();
    rm.start();

    try {
      nodeLabelsMgr.addToCluserNodeLabelsWithDefaultExclusivity(toSet("A", "B", "C"));
    } catch (IOException e) {
      Assert.fail("Caught Exception while intializing");
      e.printStackTrace();

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/nodelabels/NullRMNodeLabelsManager.java
      }

      @Override
      public void storeNewClusterNodeLabels(List<NodeLabel> label)
          throws IOException {
      }

      public void close() throws IOException {
      }
    };
  }


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/nodelabels/TestRMNodeLabelsManager.java
  
  @Test(timeout = 5000)
  public void testGetLabelResourceWhenNodeActiveDeactive() throws Exception {
    mgr.addToCluserNodeLabelsWithDefaultExclusivity(toSet("p1", "p2", "p3"));
    mgr.replaceLabelsOnNode(ImmutableMap.of(toNodeId("n1"), toSet("p1"),
        toNodeId("n2"), toSet("p2"), toNodeId("n3"), toSet("p3")));


    mgr.addToCluserNodeLabelsWithDefaultExclusivity(toSet("p1", "p4"));
    Assert.assertEquals(mgr.getResourceByLabel("p1", null),
        Resources.add(SMALL_RESOURCE, LARGE_NODE));
    Assert.assertEquals(mgr.getResourceByLabel("p4", null), EMPTY_RESOURCE);
  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Test(timeout = 5000)
  public void testGetLabelResource() throws Exception {
    mgr.addToCluserNodeLabelsWithDefaultExclusivity(toSet("p1", "p2", "p3"));
    mgr.replaceLabelsOnNode(ImmutableMap.of(toNodeId("n1"), toSet("p1"),
        toNodeId("n2"), toSet("p2"), toNodeId("n3"), toSet("p3")));

    Assert.assertEquals(mgr.getResourceByLabel("p3", null), SMALL_RESOURCE);

    mgr.addToCluserNodeLabelsWithDefaultExclusivity(toSet("p4", "p5", "p6"));
    mgr.replaceLabelsOnNode((Map) ImmutableMap.of(toNodeId("n4"), toSet("p1"),
        toNodeId("n5"), toSet("p2"), toNodeId("n6"), toSet("p3"),
        toNodeId("n7"), toSet("p4"), toNodeId("n8"), toSet("p5")));
    mgr.addToCluserNodeLabelsWithDefaultExclusivity(toSet("red", "blue", "yellow"));
    mgr.replaceLabelsOnNode(ImmutableMap.of(toNodeId("host1"), toSet("red")));
    mgr.replaceLabelsOnNode(ImmutableMap.of(toNodeId("host2"), toSet("blue")));
    mgr.replaceLabelsOnNode(ImmutableMap.of(toNodeId("host3"), toSet("yellow")));
        Resources.multiply(SMALL_RESOURCE, 4));
    
    mgr.addToCluserNodeLabelsWithDefaultExclusivity(toSet("p1"));
    mgr.addLabelsToNode(ImmutableMap.of(toNodeId("n1:1"), toSet("p1"),
        toNodeId("n1:2"), toSet("p1")));
    

  @Test(timeout = 5000)
  public void testRemoveLabelsFromNode() throws Exception {
    mgr.addToCluserNodeLabelsWithDefaultExclusivity(toSet("p1", "p2", "p3"));
    mgr.replaceLabelsOnNode(ImmutableMap.of(toNodeId("n1"), toSet("p1"),
            toNodeId("n2"), toSet("p2"), toNodeId("n3"), toSet("p3")));
  
  @Test(timeout = 5000)
  public void testGetLabelsOnNodesWhenNodeActiveDeactive() throws Exception {
    mgr.addToCluserNodeLabelsWithDefaultExclusivity(toSet("p1", "p2", "p3"));
    mgr.replaceLabelsOnNode(ImmutableMap.of(
        toNodeId("n1"), toSet("p2")));
    mgr.replaceLabelsOnNode(ImmutableMap.of(toNodeId("n1:1"), toSet("p1")));
  
  @Test(timeout = 5000)
  public void testPullRMNodeLabelsInfo() throws IOException {
    mgr.addToCluserNodeLabelsWithDefaultExclusivity(toSet("x", "y", "z"));
    mgr.activateNode(NodeId.newInstance("n1", 1), Resource.newInstance(10, 0));
    mgr.activateNode(NodeId.newInstance("n2", 1), Resource.newInstance(10, 0));
    mgr.activateNode(NodeId.newInstance("n3", 1), Resource.newInstance(10, 0));
        mgr.getLabelsToNodes(), transposeNodeToLabels(mgr.getNodeLabels()));

    mgr.addToCluserNodeLabelsWithDefaultExclusivity(toSet("p1"));
    mgr.replaceLabelsOnNode(ImmutableMap.of(toNodeId("n1"), toSet("p1")));
    Assert.assertEquals(2, mgr.getLabelsToNodes().get("p1").size());

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/TestCapacityScheduler.java
    
    final RMNodeLabelsManager mgr = new NullRMNodeLabelsManager();
    mgr.init(conf);
    mgr.addToCluserNodeLabelsWithDefaultExclusivity(ImmutableSet.of("x", "y"));
    mgr.addLabelsToNode(ImmutableMap.of(NodeId.newInstance("h1", 0), toSet("x")));
    
    MemoryRMStateStore memStore = new MemoryRMStateStore();

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/TestCapacitySchedulerNodeLabelUpdate.java
  @Test (timeout = 30000)
  public void testNodeUpdate() throws Exception {
    mgr.addToCluserNodeLabelsWithDefaultExclusivity(ImmutableSet.of("x", "y", "z"));
    

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/TestContainerAllocation.java
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.LogAggregationContext;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerAppReport;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.YarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMContainerTokenSecretManager;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.junit.Assert;

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/TestNodeLabelContainerAllocation.java
    mgr.init(conf);

    mgr.addToCluserNodeLabelsWithDefaultExclusivity(ImmutableSet.of("x", "y"));
    mgr.addLabelsToNode(ImmutableMap.of(NodeId.newInstance("h1", 0), toSet("x"),
        NodeId.newInstance("h2", 0), toSet("y")));


    mgr.addToCluserNodeLabelsWithDefaultExclusivity(ImmutableSet.of("x", "y", "z"));
    mgr.addLabelsToNode(ImmutableMap.of(NodeId.newInstance("h1", 0),
        toSet("x"), NodeId.newInstance("h2", 0), toSet("y"),
        NodeId.newInstance("h3", 0), toSet("y"), NodeId.newInstance("h4", 0),
  @Test (timeout = 120000)
  public void testContainerAllocateWithLabels() throws Exception {
    mgr.addToCluserNodeLabelsWithDefaultExclusivity(ImmutableSet.of("x", "y"));
    mgr.addLabelsToNode(ImmutableMap.of(NodeId.newInstance("h1", 0), toSet("x"),
        NodeId.newInstance("h2", 0), toSet("y")));


    mgr.addToCluserNodeLabelsWithDefaultExclusivity(ImmutableSet.of("x", "y"));
    mgr.addLabelsToNode(ImmutableMap.of(NodeId.newInstance("h1", 0), toSet("x"),
        NodeId.newInstance("h2", 0), toSet("y")));

    
    mgr.addToCluserNodeLabels(ImmutableSet.of(
        NodeLabel.newInstance("x"), NodeLabel.newInstance("y", false)));
    mgr.addLabelsToNode(ImmutableMap.of(NodeId.newInstance("h1", 0), toSet("y")));

    
    mgr.addToCluserNodeLabels(ImmutableSet.of(
        NodeLabel.newInstance("x"), NodeLabel.newInstance("y", false)));
    mgr.addLabelsToNode(ImmutableMap.of(NodeId.newInstance("h1", 0), toSet("y")));

    
    mgr.addToCluserNodeLabels(ImmutableSet.of(
        NodeLabel.newInstance("x", false), NodeLabel.newInstance("y")));
    mgr.addLabelsToNode(ImmutableMap.of(NodeId.newInstance("h1", 0), toSet("x")));

    csConf.setCapacityByLabel(C2, "x", 0);
    
    mgr.addToCluserNodeLabels(ImmutableSet.of(
        NodeLabel.newInstance("x", false), NodeLabel.newInstance("y")));
    mgr.addLabelsToNode(ImmutableMap.of(NodeId.newInstance("h1", 0), toSet("x")));

    csConf.setUserLimitFactor(B, 5);
    
    mgr.addToCluserNodeLabels(ImmutableSet.of(
        NodeLabel.newInstance("x", false), NodeLabel.newInstance("y")));
    mgr.addLabelsToNode(ImmutableMap.of(NodeId.newInstance("h1", 0), toSet("x")));

    
    mgr.addToCluserNodeLabels(ImmutableSet.of(
        NodeLabel.newInstance("x", false), NodeLabel.newInstance("y")));
    mgr.addLabelsToNode(ImmutableMap.of(NodeId.newInstance("h1", 0), toSet("x")));

    csConf.setMaximumCapacityByLabel(B, "x", 50);

    mgr.addToCluserNodeLabels(ImmutableSet.of(
        NodeLabel.newInstance("x", false)));
    mgr.addLabelsToNode(ImmutableMap.of(NodeId.newInstance("h1", 0), toSet("x")));

    csConf.setCapacityByLabel(B, "x", 50);

    mgr.addToCluserNodeLabels(ImmutableSet.of(
        NodeLabel.newInstance("x", false)));
    mgr.addLabelsToNode(ImmutableMap.of(NodeId.newInstance("h1", 0), toSet("x")));

    csConf.setCapacity(D, 25);

    mgr.addToCluserNodeLabels(ImmutableSet.of(
        NodeLabel.newInstance("x", false)));
    mgr.addLabelsToNode(ImmutableMap.of(NodeId.newInstance("h1", 0), toSet("x")));


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/TestQueueParsing.java
  
  @Test
  public void testQueueParsingReinitializeWithLabels() throws IOException {
    nodeLabelManager.addToCluserNodeLabelsWithDefaultExclusivity(ImmutableSet.of("red", "blue"));
    CapacitySchedulerConfiguration csConf =
        new CapacitySchedulerConfiguration();
    setupQueueConfigurationWithoutLabels(csConf);
  
  @Test
  public void testQueueParsingWithLabels() throws IOException {
    nodeLabelManager.addToCluserNodeLabelsWithDefaultExclusivity(ImmutableSet.of("red", "blue"));
    
    YarnConfiguration conf = new YarnConfiguration();
    CapacitySchedulerConfiguration csConf =
  
  @Test
  public void testQueueParsingWithLabelsInherit() throws IOException {
    nodeLabelManager.addToCluserNodeLabelsWithDefaultExclusivity(ImmutableSet.of("red", "blue"));

    YarnConfiguration conf = new YarnConfiguration();
    CapacitySchedulerConfiguration csConf =
    
    nodeLabelManager.addToCluserNodeLabelsWithDefaultExclusivity(labels);

    CapacitySchedulerConfiguration csConf =
        new CapacitySchedulerConfiguration();

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/TestWorkPreservingRMRestartForNodeLabel.java

    mgr.addToCluserNodeLabelsWithDefaultExclusivity(ImmutableSet.of("x", "y"));
    mgr.addLabelsToNode(ImmutableMap.of(NodeId.newInstance("h1", 0), toSet("x"),
        NodeId.newInstance("h2", 0), toSet("y")));

    mgr = new NullRMNodeLabelsManager();
    mgr.init(conf);
    mgr.addToCluserNodeLabelsWithDefaultExclusivity(ImmutableSet.of("x", "y"));
    mgr.addLabelsToNode(ImmutableMap.of(NodeId.newInstance("h1", 0), toSet("x"),
        NodeId.newInstance("h2", 0), toSet("y")));
    MockRM rm2 =

