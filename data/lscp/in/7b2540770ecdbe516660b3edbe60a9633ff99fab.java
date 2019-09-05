hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/main/java/org/apache/hadoop/yarn/nodelabels/FileSystemNodeLabelsStore.java
  @Override
  public void updateNodeToLabelsMappings(
      Map<NodeId, Set<String>> nodeToLabels) throws IOException {
    try {
      ensureAppendEditlogFile();
      editlogOs.writeInt(SerializedLogType.NODE_TO_LABELS.ordinal());
      ((ReplaceLabelsOnNodeRequestPBImpl) ReplaceLabelsOnNodeRequest
          .newInstance(nodeToLabels)).getProto().writeDelimitedTo(editlogOs);
    } finally {
      ensureCloseEditlogFile();
    }
  }

  @Override
  public void storeNewClusterNodeLabels(List<NodeLabel> labels)
      throws IOException {
    try {
      ensureAppendEditlogFile();
      editlogOs.writeInt(SerializedLogType.ADD_LABELS.ordinal());
      ((AddToClusterNodeLabelsRequestPBImpl) AddToClusterNodeLabelsRequest
          .newInstance(labels)).getProto().writeDelimitedTo(editlogOs);
    } finally {
      ensureCloseEditlogFile();
    }
  }

  @Override
  public void removeClusterNodeLabels(Collection<String> labels)
      throws IOException {
    try {
      ensureAppendEditlogFile();
      editlogOs.writeInt(SerializedLogType.REMOVE_LABELS.ordinal());
      ((RemoveFromClusterNodeLabelsRequestPBImpl) RemoveFromClusterNodeLabelsRequest.newInstance(Sets
          .newHashSet(labels.iterator()))).getProto().writeDelimitedTo(editlogOs);
    } finally {
      ensureCloseEditlogFile();
    }
  }


