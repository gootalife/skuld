hadoop-yarn-project/hadoop-yarn/hadoop-yarn-client/src/main/java/org/apache/hadoop/yarn/client/cli/ClusterCLI.java
        "List cluster node-label collection");
    opts.addOption("h", HELP_CMD, false, "Displays help for all commands.");
    opts.addOption("dnl", DIRECTLY_ACCESS_NODE_LABEL_STORE, false,
        "This is DEPRECATED, will be removed in future releases. Directly access node label store, "
            + "with this option, all node label related operations"
            + " will NOT connect RM. Instead, they will"
            + " access/modify stored node labels directly."

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-client/src/main/java/org/apache/hadoop/yarn/client/cli/RMAdminCLI.java
          .put("-getGroups", new UsageInfo("[username]",
              "Get the groups which given user belongs to."))
          .put("-addToClusterNodeLabels",
              new UsageInfo("<\"label1(exclusive=true),"
                  + "label2(exclusive=false),label3\">",
                  "add to cluster node labels. Default exclusivity is true"))
          .put("-removeFromClusterNodeLabels",
              new UsageInfo("<label1,label2,label3> (label splitted by \",\")",
                  "remove from cluster node labels"))
          .put("-replaceLabelsOnNode",
              new UsageInfo(
                  "<\"node1[:port]=label1,label2 node2[:port]=label1,label2\">",
                  "replace labels on nodes"
                      + " (please note that we do not support specifying multiple"
                      + " labels on a single host for now.)"))
          .put("-directlyAccessNodeLabelStore",
              new UsageInfo("", "This is DEPRECATED, will be removed in future releases. Directly access node label store, "
                  + "with this option, all node label related operations"
                  + " will not connect RM. Instead, they will"
                  + " access/modify stored node labels directly."
      " [-refreshAdminAcls]" +
      " [-refreshServiceAcl]" +
      " [-getGroup [username]]" +
      " [-addToClusterNodeLabels <\"label1(exclusive=true),"
                  + "label2(exclusive=false),label3\">]" +
      " [-removeFromClusterNodeLabels <label1,label2,label3>]" +
      " [-replaceLabelsOnNode <\"node1[:port]=label1,label2 node2[:port]=label1\">]" +
      " [-directlyAccessNodeLabelStore]]");
    if (isHAEnabled) {
      appendHAUsage(summary);
      } else if ("-addToClusterNodeLabels".equals(cmd)) {
        if (i >= args.length) {
          System.err.println(NO_LABEL_ERR_MSG);
          printUsage("", isHAEnabled);
          exitCode = -1;
        } else {
          exitCode = addToClusterNodeLabels(args[i]);
      } else if ("-removeFromClusterNodeLabels".equals(cmd)) {
        if (i >= args.length) {
          System.err.println(NO_LABEL_ERR_MSG);
          printUsage("", isHAEnabled);
          exitCode = -1;
        } else {
          exitCode = removeFromClusterNodeLabels(args[i]);
      } else if ("-replaceLabelsOnNode".equals(cmd)) {
        if (i >= args.length) {
          System.err.println(NO_MAPPING_ERR_MSG);
          printUsage("", isHAEnabled);
          exitCode = -1;
        } else {
          exitCode = replaceLabelsOnNodes(args[i]);

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-client/src/test/java/org/apache/hadoop/yarn/client/cli/TestClusterCLI.java
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintWriter pw = new PrintWriter(baos);
    pw.println("usage: yarn cluster");
    pw.println(" -dnl,--directly-access-node-label-store   This is DEPRECATED, will be");
    pw.println("                                           removed in future releases.");
    pw.println("                                           Directly access node label");
    pw.println("                                           store, with this option, all");
    pw.println("                                           node label related operations");
    pw.println("                                           will NOT connect RM. Instead,");

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-client/src/test/java/org/apache/hadoop/yarn/client/cli/TestRMAdminCLI.java
              "yarn rmadmin [-refreshQueues] [-refreshNodes [-g [timeout in seconds]]] [-refreshSuper" +
              "UserGroupsConfiguration] [-refreshUserToGroupsMappings] " +
              "[-refreshAdminAcls] [-refreshServiceAcl] [-getGroup" +
              " [username]] [-addToClusterNodeLabels <\"label1(exclusive=true),label2(exclusive=false),label3\">]" +
              " [-removeFromClusterNodeLabels <label1,label2,label3>] [-replaceLabelsOnNode " +
              "<\"node1[:port]=label1,label2 node2[:port]=label1\">] [-directlyAccessNodeLabelStore]] " +
              "[-help [cmd]]"));
      assertTrue(dataOut
          .toString()
          "yarn rmadmin [-refreshQueues] [-refreshNodes [-g [timeout in seconds]]] [-refreshSuper"
              + "UserGroupsConfiguration] [-refreshUserToGroupsMappings] "
              + "[-refreshAdminAcls] [-refreshServiceAcl] [-getGroup"
              + " [username]] [-addToClusterNodeLabels <\"label1(exclusive=true),"
                  + "label2(exclusive=false),label3\">]"
              + " [-removeFromClusterNodeLabels <label1,label2,label3>] [-replaceLabelsOnNode "
              + "<\"node1[:port]=label1,label2 node2[:port]=label1\">] [-directlyAccessNodeLabelStore]] "
              + "[-transitionToActive [--forceactive] <serviceId>] "
              + "[-transitionToStandby <serviceId>] "
              + "[-getServiceState <serviceId>] [-checkHealth <serviceId>] [-help [cmd]]";

