hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/TestClientRMService.java
    RMNodeLabelsManager labelsMgr = rm.getRMContext().getNodeLabelManager();
    labelsMgr.addToCluserNodeLabelsWithDefaultExclusivity(ImmutableSet.of("x", "y"));

    NodeId node1 = NodeId.newInstance("host1", 1234);
    NodeId node2 = NodeId.newInstance("host2", 1234);
    Map<NodeId, Set<String>> map = new HashMap<NodeId, Set<String>>();
    map.put(node1, ImmutableSet.of("x"));
    map.put(node2, ImmutableSet.of("y"));
    labelsMgr.replaceLabelsOnNode(map);

        client.getNodeToLabels(GetNodesToLabelsRequest.newInstance());
    Map<NodeId, Set<String>> nodeToLabels = response1.getNodeToLabels();
    Assert.assertTrue(nodeToLabels.keySet().containsAll(
        Arrays.asList(node1, node2)));
    Assert.assertTrue(nodeToLabels.get(node1).containsAll(Arrays.asList("x")));
    Assert.assertTrue(nodeToLabels.get(node2).containsAll(Arrays.asList("y")));
    
    rpc.stopProxy(client, conf);
    rm.close();
    RMNodeLabelsManager labelsMgr = rm.getRMContext().getNodeLabelManager();
    labelsMgr.addToCluserNodeLabelsWithDefaultExclusivity(ImmutableSet.of("x", "y", "z"));

    NodeId node1A = NodeId.newInstance("host1", 1234);
    NodeId node1B = NodeId.newInstance("host1", 5678);
    NodeId node2A = NodeId.newInstance("host2", 1234);
    NodeId node3A = NodeId.newInstance("host3", 1234);
    NodeId node3B = NodeId.newInstance("host3", 5678);
    Map<NodeId, Set<String>> map = new HashMap<NodeId, Set<String>>();
    map.put(node1A, ImmutableSet.of("x"));
    map.put(node1B, ImmutableSet.of("z"));
    map.put(node2A, ImmutableSet.of("y"));
    map.put(node3A, ImmutableSet.of("y"));
    map.put(node3B, ImmutableSet.of("z"));
    labelsMgr.replaceLabelsOnNode(map);

    Assert.assertTrue(
        labelsToNodes.keySet().containsAll(Arrays.asList("x", "y", "z")));
    Assert.assertTrue(
        labelsToNodes.get("x").containsAll(Arrays.asList(node1A)));
    Assert.assertTrue(
        labelsToNodes.get("y").containsAll(Arrays.asList(node2A, node3A)));
    Assert.assertTrue(
        labelsToNodes.get("z").containsAll(Arrays.asList(node1B, node3B)));

    Set<String> setlabels =
    Assert.assertTrue(
        labelsToNodes.keySet().containsAll(Arrays.asList("x", "z")));
    Assert.assertTrue(
        labelsToNodes.get("x").containsAll(Arrays.asList(node1A)));
    Assert.assertTrue(
        labelsToNodes.get("z").containsAll(Arrays.asList(node1B, node3B)));
    Assert.assertEquals(labelsToNodes.get("y"), null);

    rpc.stopProxy(client, conf);

