hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/nodelabels/RMNodeLabelsManager.java

public class RMNodeLabelsManager extends CommonNodeLabelsManager {
  protected static class Queue {
    protected Set<String> accessibleNodeLabels;
    protected Resource resource;

    protected Queue() {
      accessibleNodeLabels =
          Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
      resource = Resource.newInstance(0, 0);
    }
      for (Entry<String, Queue> entry : queueCollections.entrySet()) {
        String queueName = entry.getKey();
        Set<String> queueLabels = entry.getValue().accessibleNodeLabels;
        if (queueLabels.contains(label)) {
          throw new IOException("Cannot remove label=" + label
              + ", because queue=" + queueName + " is using this label. "
          continue;
        }

        q.accessibleNodeLabels.addAll(labels);
        for (Host host : nodeCollections.values()) {
          for (Entry<NodeId, Node> nentry : host.nms.entrySet()) {
            NodeId nodeId = nentry.getKey();
    }

    for (String label : nodeLabels) {
      if (q.accessibleNodeLabels.contains(label)) {
        return true;
      }
    }

