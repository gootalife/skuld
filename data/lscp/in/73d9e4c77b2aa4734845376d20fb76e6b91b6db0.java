hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/webapp/NodesPage.java
      String type = $(NODE_STATE);
      String labelFilter = $(NODE_LABEL, CommonNodeLabelsManager.ANY).trim();
      TBODY<TABLE<Hamlet>> tbody =
          html.table("#nodes").thead().tr()
              .th(".nodelabels", "Node Labels")
              .th(".rack", "Rack")
              .th(".state", "Node State")
              .th(".nodeaddress", "Node Address")
              .th(".nodehttpaddress", "Node HTTP Address")
              .th(".lastHealthUpdate", "Last health-update")
              .th(".healthReport", "Health-report")
              .th(".containers", "Containers")
              .th(".mem", "Mem Used")
              .th(".mem", "Mem Avail")
              .th(".vcores", "VCores Used")
              .th(".vcores", "VCores Avail")
              .th(".nodeManagerVersion", "Version")._()._().tbody();
      NodeState stateFilter = null;

  private String nodesTableInit() {
    StringBuilder b = tableInit().append(", aoColumnDefs: [");
    b.append("{'bSearchable': false, 'aTargets': [ 7 ]}");
    b.append(", {'sType': 'title-numeric', 'bSearchable': false, "
        + "'aTargets': [ 8, 9 ] }");
    b.append(", {'sType': 'title-numeric', 'aTargets': [ 5 ]}");
    b.append("]}");
    return b.toString();
  }

