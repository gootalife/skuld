hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/net/NetworkTopology.java
package org.apache.hadoop.net;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.locks.ReadWriteLock;
  static class InnerNode extends NodeBase {
    protected List<Node> children=new ArrayList<Node>();
    private Map<String, Node> childrenMap = new HashMap<String, Node>();
    private int numOfLeaves;
        
        n.setParent(this);
        n.setLevel(this.level+1);
        Node prev = childrenMap.put(n.getName(), n);
        if (prev != null) {
          for(int i=0; i<children.size(); i++) {
            if (children.get(i).getName().equals(n.getName())) {
              children.set(i, n);
              return false;
            }
          }
        }
        children.add(n);
        numOfLeaves++;
        return true;
      } else {
        String parentName = getNextAncestorName(n);
        InnerNode parentNode = (InnerNode)childrenMap.get(parentName);
        if (parentNode == null) {
          parentNode = createParentNode(parentName);
          children.add(parentNode);
          childrenMap.put(parentNode.getName(), parentNode);
        }
        if (parentNode.add(n)) {
                                           +parent+", is not a descendent of "+currentPath);
      if (isParent(n)) {
        if (childrenMap.containsKey(n.getName())) {
          for (int i=0; i<children.size(); i++) {
            if (children.get(i).getName().equals(n.getName())) {
              children.remove(i);
              childrenMap.remove(n.getName());
              numOfLeaves--;
              n.setParent(null);
              return true;
            }
          }
        }
        return false;
      } else {
        if (isRemoved) {
          if (parentNode.getNumOfChildren() == 0) {
            Node prev = children.remove(i);
            childrenMap.remove(prev.getName());
          }
          numOfLeaves--;
        }
      if (loc == null || loc.length() == 0) return this;
            
      String[] path = loc.split(PATH_SEPARATOR_STR, 2);
      Node childnode = childrenMap.get(path[0]);
      if (childnode == null) return null; // non-existing node
      if (path.length == 1) return childnode;
      if (childnode instanceof InnerNode) {
        isLeaf ? 1 : ((InnerNode)excludedNode).getNumOfLeaves();
      if (isLeafParent()) { // children are leaves
        if (isLeaf) { // excluded node is a leaf node
          if (excludedNode != null &&
              childrenMap.containsKey(excludedNode.getName())) {
            int excludedIndex = children.indexOf(excludedNode);
            if (excludedIndex != -1 && leafIndex >= 0) {
              leafIndex = leafIndex>=excludedIndex ? leafIndex+1 : leafIndex;
            }
          }
        }
        if (leafIndex<0 || leafIndex>=this.getNumOfChildren()) {
          return null;

hadoop-common-project/hadoop-common/src/test/java/org/apache/hadoop/net/TestClusterTopology.java
package org.apache.hadoop.net;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.math3.stat.inference.ChiSquareTest;
import org.junit.Assert;
import org.junit.Test;

  public void testCountNumNodes() throws Exception {
    NetworkTopology cluster = new NetworkTopology();
    NodeElement node1 = getNewNode("node1", "/d1/r1");
    cluster.add(node1);
    NodeElement node2 = getNewNode("node2", "/d1/r2");
    cluster.add(node2);
    NodeElement node3 = getNewNode("node3", "/d1/r3");
    cluster.add(node3);
    NodeElement node4 = getNewNode("node4", "/d1/r4");
    cluster.add(node4);
    List<Node> excludedNodes = new ArrayList<Node>();

    assertEquals("4 nodes should be available with extra excluded Node", 4,
        cluster.countNumOfAvailableNodes(NodeBase.ROOT, excludedNodes));
    excludedNodes.add(node4);
    assertEquals("excluded nodes with ROOT scope should be considered", 3,
        cluster.countNumOfAvailableNodes(NodeBase.ROOT, excludedNodes));
    assertEquals("excluded nodes without ~ scope should be considered", 2,
    assertEquals("No nodes should be considered for non-exist scope", 0,
        cluster.countNumOfAvailableNodes("/non-exist", excludedNodes));
    cluster.remove(node1);
    assertEquals("1 node should be available", 1,
        cluster.countNumOfAvailableNodes(NodeBase.ROOT, excludedNodes));
  }

  @Test
  public void testChooseRandom() {
    NetworkTopology cluster = new NetworkTopology();
    NodeElement node1 = getNewNode("node1", "/d1/r1");
    cluster.add(node1);
    NodeElement node2 = getNewNode("node2", "/d1/r2");
    cluster.add(node2);
    NodeElement node3 = getNewNode("node3", "/d1/r3");
    cluster.add(node3);
    NodeElement node4 = getNewNode("node4", "/d1/r3");
    cluster.add(node4);

    int numIterations = 100;

    HashMap<String,Integer> histogram = new HashMap<String,Integer>();
    for (int i=0; i<numIterations; i++) {
      String randomNode = cluster.chooseRandom(NodeBase.ROOT).getName();
      if (!histogram.containsKey(randomNode)) {
        histogram.put(randomNode, 0);
      }
      histogram.put(randomNode, histogram.get(randomNode) + 1);
    }
    assertEquals("Random is not selecting all nodes", 4, histogram.size());

    ChiSquareTest chiSquareTest = new ChiSquareTest();
    double[] expected = new double[histogram.size()];
    long[] observed = new long[histogram.size()];
    int j=0;
    for (Integer occurrence : histogram.values()) {
      expected[j] = 1.0 * numIterations / histogram.size();
      observed[j] = occurrence;
      j++;
    }
    boolean chiSquareTestRejected =
        chiSquareTest.chiSquareTest(expected, observed, 0.01);

    assertFalse("Not choosing nodes randomly", chiSquareTestRejected);

    histogram = new HashMap<String,Integer>();
    for (int i=0; i<numIterations; i++) {
      String randomNode = cluster.chooseRandom("~/d1/r3").getName();
      if (!histogram.containsKey(randomNode)) {
        histogram.put(randomNode, 0);
      }
      histogram.put(randomNode, histogram.get(randomNode) + 1);
    }
    assertEquals("Random is not selecting the nodes it should",
        2, histogram.size());
  }

  private NodeElement getNewNode(String name, String rackLocation) {

