hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/main/java/org/apache/hadoop/yarn/nodelabels/CommonNodeLabelsManager.java
  private static final int MAX_LABEL_LENGTH = 255;
  public static final Set<String> EMPTY_STRING_SET = Collections
      .unmodifiableSet(new HashSet<String>(0));
  public static final Set<NodeLabel> EMPTY_NODELABEL_SET = Collections
      .unmodifiableSet(new HashSet<NodeLabel>(0));
  public static final String ANY = "*";
  public static final Set<String> ACCESS_ANY_LABEL_SET = ImmutableSet.of(ANY);
  private static final Pattern LABEL_PATTERN = Pattern
  public Map<NodeId, Set<String>> getNodeLabels() {
    Map<NodeId, Set<String>> nodeToLabels =
        generateNodeLabelsInfoPerNode(String.class);
    return nodeToLabels;
  }

  public Map<NodeId, Set<NodeLabel>> getNodeLabelsInfo() {
    Map<NodeId, Set<NodeLabel>> nodeToLabels =
        generateNodeLabelsInfoPerNode(NodeLabel.class);
    return nodeToLabels;
  }

  @SuppressWarnings("unchecked")
  private <T> Map<NodeId, Set<T>> generateNodeLabelsInfoPerNode(Class<T> type) {
    try {
      readLock.lock();
      Map<NodeId, Set<T>> nodeToLabels = new HashMap<>();
      for (Entry<String, Host> entry : nodeCollections.entrySet()) {
        String hostName = entry.getKey();
        Host host = entry.getValue();
        for (NodeId nodeId : host.nms.keySet()) {
          if (type.isAssignableFrom(String.class)) {
            Set<String> nodeLabels = getLabelsByNode(nodeId);
            if (nodeLabels == null || nodeLabels.isEmpty()) {
              continue;
            }
            nodeToLabels.put(nodeId, (Set<T>) nodeLabels);
          } else {
            Set<NodeLabel> nodeLabels = getLabelsInfoByNode(nodeId);
            if (nodeLabels == null || nodeLabels.isEmpty()) {
              continue;
            }
            nodeToLabels.put(nodeId, (Set<T>) nodeLabels);
          }
        }
        if (!host.labels.isEmpty()) {
          if (type.isAssignableFrom(String.class)) {
            nodeToLabels.put(NodeId.newInstance(hostName, WILDCARD_PORT),
                (Set<T>) host.labels);
          } else {
            nodeToLabels.put(NodeId.newInstance(hostName, WILDCARD_PORT),
                (Set<T>) createNodeLabelFromLabelNames(host.labels));
          }
        }
      }
      return Collections.unmodifiableMap(nodeToLabels);
  public Map<String, Set<NodeId>> getLabelsToNodes(Set<String> labels) {
    try {
      readLock.lock();
      Map<String, Set<NodeId>> labelsToNodes = getLabelsToNodesMapping(labels,
          String.class);
      return Collections.unmodifiableMap(labelsToNodes);
    } finally {
      readLock.unlock();
    }
  }


  public Map<NodeLabel, Set<NodeId>> getLabelsInfoToNodes() {
    try {
      readLock.lock();
      return getLabelsInfoToNodes(labelCollections.keySet());
    } finally {
      readLock.unlock();
    }
  }

  public Map<NodeLabel, Set<NodeId>> getLabelsInfoToNodes(Set<String> labels) {
    try {
      readLock.lock();
      Map<NodeLabel, Set<NodeId>> labelsToNodes = getLabelsToNodesMapping(
          labels, NodeLabel.class);
      return Collections.unmodifiableMap(labelsToNodes);
    } finally {
      readLock.unlock();
    }
  }

  private <T> Map<T, Set<NodeId>> getLabelsToNodesMapping(Set<String> labels,
      Class<T> type) {
    Map<T, Set<NodeId>> labelsToNodes = new HashMap<T, Set<NodeId>>();
    for (String label : labels) {
      if (label.equals(NO_LABEL)) {
        continue;
      if (nodeLabelInfo != null) {
        Set<NodeId> nodeIds = nodeLabelInfo.getAssociatedNodeIds();
        if (!nodeIds.isEmpty()) {
          if (type.isAssignableFrom(String.class)) {
            labelsToNodes.put(type.cast(label), nodeIds);
          } else {
            labelsToNodes.put(type.cast(nodeLabelInfo.getNodeLabel()), nodeIds);
          }
        }
      } else {
        LOG.warn("getLabelsToNodes : Label [" + label + "] cannot be found");
      }
    }
    return labelsToNodes;
  }

    }
  }
  
  private Set<NodeLabel> getLabelsInfoByNode(NodeId nodeId) {
    Set<String> labels = getLabelsByNode(nodeId, nodeCollections);
    if (labels.isEmpty()) {
      return EMPTY_NODELABEL_SET;
    }
    Set<NodeLabel> nodeLabels = createNodeLabelFromLabelNames(labels);
    return nodeLabels;
  }

  private Set<NodeLabel> createNodeLabelFromLabelNames(Set<String> labels) {
    Set<NodeLabel> nodeLabels = new HashSet<NodeLabel>();
    for (String label : labels) {
      if (label.equals(NO_LABEL)) {
        continue;
      }
      RMNodeLabel rmLabel = labelCollections.get(label);
      if (rmLabel == null) {
        continue;
      }
      nodeLabels.add(rmLabel.getNodeLabel());
    }
    return nodeLabels;
  }

  protected void createNodeIfNonExisted(NodeId nodeId) throws IOException {
    Host host = nodeCollections.get(nodeId.getHost());
    if (null == host) {

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/main/java/org/apache/hadoop/yarn/nodelabels/RMNodeLabel.java
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.resource.Resources;

public class RMNodeLabel implements Comparable<RMNodeLabel> {
  private String labelName;
  private Set<NodeId> nodeIds;
  private boolean exclusive;
  private NodeLabel nodeLabel;

  public RMNodeLabel(NodeLabel nodeLabel) {
    this(nodeLabel.getName(), Resource.newInstance(0, 0), 0,
    this.numActiveNMs = activeNMs;
    this.nodeIds = new HashSet<NodeId>();
    this.exclusive = exclusive;
    this.nodeLabel = NodeLabel.newInstance(labelName, exclusive);
  }

  public void addNodeId(NodeId node) {
    return new RMNodeLabel(labelName, resource, numActiveNMs, exclusive);
  }
  
  public NodeLabel getNodeLabel() {
    return this.nodeLabel;
  }

  @Override
  public int compareTo(RMNodeLabel o) {

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/test/java/org/apache/hadoop/yarn/nodelabels/NodeLabelTestBase.java
import java.util.Map.Entry;

import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.junit.Assert;

import com.google.common.collect.ImmutableMap;
    }
  }

  public static void assertLabelInfoMapEquals(Map<NodeId, Set<NodeLabel>> m1,
      ImmutableMap<NodeId, Set<NodeLabel>> m2) {
    Assert.assertEquals(m1.size(), m2.size());
    for (NodeId k : m1.keySet()) {
      Assert.assertTrue(m2.containsKey(k));
      assertNLCollectionEquals(m1.get(k), m2.get(k));
    }
  }

  public static void assertLabelsToNodesEquals(Map<String, Set<NodeId>> m1,
      ImmutableMap<String, Set<NodeId>> m2) {
    Assert.assertEquals(m1.size(), m2.size());
    Assert.assertTrue(s1.containsAll(s2));
  }

  public static void assertNLCollectionEquals(Collection<NodeLabel> c1,
      Collection<NodeLabel> c2) {
    Set<NodeLabel> s1 = new HashSet<NodeLabel>(c1);
    Set<NodeLabel> s2 = new HashSet<NodeLabel>(c2);
    Assert.assertEquals(s1, s2);
    Assert.assertTrue(s1.containsAll(s2));
  }

  @SuppressWarnings("unchecked")
  public static <E> Set<E> toSet(E... elements) {
    Set<E> set = Sets.newHashSet(elements);
      return NodeId.newInstance(str, CommonNodeLabelsManager.WILDCARD_PORT);
    }
  }

  public static void assertLabelsInfoToNodesEquals(
      Map<NodeLabel, Set<NodeId>> m1, ImmutableMap<NodeLabel, Set<NodeId>> m2) {
    Assert.assertEquals(m1.size(), m2.size());
    for (NodeLabel k : m1.keySet()) {
      Assert.assertTrue(m2.containsKey(k));
      Set<NodeId> s1 = new HashSet<NodeId>(m1.get(k));
      Set<NodeId> s2 = new HashSet<NodeId>(m2.get(k));
      Assert.assertEquals(s1, s2);
      Assert.assertTrue(s1.containsAll(s2));
    }
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/test/java/org/apache/hadoop/yarn/nodelabels/TestCommonNodeLabelsManager.java
        labelsByNode);
    Assert.assertTrue(labelsByNode.contains("p1"));
  }

  @Test(timeout = 5000)
  public void testLabelsInfoToNodes() throws IOException {
    mgr.addToCluserNodeLabels(Arrays.asList(NodeLabel.newInstance("p1", false),
        NodeLabel.newInstance("p2", true), NodeLabel.newInstance("p3", true)));
    mgr.addLabelsToNode(ImmutableMap.of(toNodeId("n1"), toSet("p1")));
    Map<NodeLabel, Set<NodeId>> labelsToNodes = mgr.getLabelsInfoToNodes();
    assertLabelsInfoToNodesEquals(labelsToNodes, ImmutableMap.of(
        NodeLabel.newInstance("p1", false), toSet(toNodeId("n1"))));
  }

  @Test(timeout = 5000)
  public void testGetNodeLabelsInfo() throws IOException {
    mgr.addToCluserNodeLabels(Arrays.asList(NodeLabel.newInstance("p1", false),
        NodeLabel.newInstance("p2", true), NodeLabel.newInstance("p3", false)));
    mgr.addLabelsToNode(ImmutableMap.of(toNodeId("n1"), toSet("p2")));
    mgr.addLabelsToNode(ImmutableMap.of(toNodeId("n2"), toSet("p3")));

    assertLabelInfoMapEquals(mgr.getNodeLabelsInfo(), ImmutableMap.of(
        toNodeId("n1"), toSet(NodeLabel.newInstance("p2", true)),
        toNodeId("n2"), toSet(NodeLabel.newInstance("p3", false))));
  }
}

