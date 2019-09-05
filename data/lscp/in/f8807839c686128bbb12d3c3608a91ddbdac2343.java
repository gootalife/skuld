hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/webapp/NodeLabelsPage.java
          thead().
          tr().
          th(".name", "Label Name").
          th(".type", "Label Type").
          th(".numOfActiveNMs", "Num Of Active NMs").
          th(".totalResource", "Total Resource").
          _()._().
      for (RMNodeLabel info : nlm.pullRMNodeLabelsInfo()) {
        TR<TBODY<TABLE<Hamlet>>> row =
            tbody.tr().td(
                info.getLabelName().isEmpty() ? "<DEFAULT_PARTITION>" : info
                    .getLabelName());
        String type =
            (info.getIsExclusive()) ? "Exclusive Partition"
                : "Non Exclusive Partition";
        row = row.td(type);
        int nActiveNMs = info.getNumActiveNMs();
        if (nActiveNMs > 0) {
          row = row.td()

