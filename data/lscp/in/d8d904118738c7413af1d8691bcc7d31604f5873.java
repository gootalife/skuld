hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/TestClusterId.java
    NameNode.createNameNode(argv, config);

    assertTrue(baos.toString("UTF-8").contains("Usage: hdfs namenode"));
    System.setErr(origErr);

    NameNode.createNameNode(argv, config);

    assertTrue(baos.toString("UTF-8").contains("Usage: hdfs namenode"));
    System.setErr(origErr);

    NameNode.createNameNode(argv, config);

    assertTrue(baos.toString("UTF-8").contains("Usage: hdfs namenode"));
    System.setErr(origErr);


