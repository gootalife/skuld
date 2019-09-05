hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/test/java/org/apache/hadoop/yarn/server/nodemanager/TestLocalDirsHandlerService.java
    FileUtils.deleteDirectory(new File(localDir1));
    FileUtils.deleteDirectory(new File(localDir2));
    FileUtils.deleteDirectory(new File(logDir1));
    FileUtils.deleteDirectory(new File(logDir2));
    dirSvc.close();
  }
}

