hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSImage.java
        true);
    archivalManager.purgeCheckpoinsAfter(NameNodeFile.IMAGE, ckptId);
    archivalManager.purgeCheckpoints(NameNodeFile.IMAGE_ROLLBACK);
    String nameserviceId = DFSUtil.getNamenodeNameServiceId(conf);
    if (HAUtil.isHAEnabled(conf, nameserviceId)) {

