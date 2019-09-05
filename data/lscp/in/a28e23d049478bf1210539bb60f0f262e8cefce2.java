hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/main/java/org/apache/hadoop/yarn/nodelabels/CommonNodeLabelsManager.java
    }
    List<NodeLabel> newLabels = new ArrayList<NodeLabel>();
    normalizeNodeLabels(labels);
    checkExclusivityMatch(labels);
    for (NodeLabel label : labels) {
    }
  }

  private void checkExclusivityMatch(Collection<NodeLabel> labels)
      throws IOException {
    ArrayList<NodeLabel> mismatchlabels = new ArrayList<NodeLabel>();
    for (NodeLabel label : labels) {
      RMNodeLabel rmNodeLabel = this.labelCollections.get(label.getName());
      if (rmNodeLabel != null
          && rmNodeLabel.getIsExclusive() != label.isExclusive()) {
        mismatchlabels.add(label);
      }
    }
    if (mismatchlabels.size() > 0) {
      throw new IOException(
          "Exclusivity cannot be modified for an existing label with : "
              + StringUtils.join(mismatchlabels.iterator(), ","));
    }
  }

  protected String normalizeLabel(String label) {
    if (label != null) {
      return label.trim();

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/test/java/org/apache/hadoop/yarn/nodelabels/TestCommonNodeLabelsManager.java

    Assert.assertTrue(mgr.getClusterNodeLabelNames().containsAll(
        Sets.newHashSet("hello", "world", "hello1", "world1")));
    try {
      mgr.addToCluserNodeLabels(Arrays.asList(NodeLabel.newInstance("hello1",
          false)));
      Assert.fail("IOException not thrown on exclusivity change of labels");
    } catch (Exception e) {
      Assert.assertTrue("IOException is expected when exclusivity is modified",
          e instanceof IOException);
    }
    try {
      mgr.addToCluserNodeLabels(Arrays.asList(NodeLabel.newInstance("hello1",
          true)));
    } catch (Exception e) {
      Assert.assertFalse(
          "IOException not expected when no change in exclusivity",
          e instanceof IOException);
    }
    for (String p : Arrays.asList(null, CommonNodeLabelsManager.NO_LABEL, "xx")) {
      boolean caught = false;

