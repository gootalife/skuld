hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/tools/DFSAdmin.java

    String report ="-report [-live] [-dead] [-decommissioning]:\n" +
      "\tReports basic filesystem information and statistics. \n" +
      "\tThe dfs usage can be different from \"du\" usage, because it\n" +
      "\tmeasures raw space used by replication, checksums, snapshots\n" +
      "\tand etc. on all the DNs.\n" +
      "\tOptional flags may be used to filter the list of displayed DNs.\n";

    String safemode = "-safemode <enter|leave|get|wait>:  Safe mode maintenance command.\n" + 
      "\t\tSafe mode is a Namenode state in which it\n" +
      "\t\t\t1.  does not accept changes to the name space (read-only)\n" +

