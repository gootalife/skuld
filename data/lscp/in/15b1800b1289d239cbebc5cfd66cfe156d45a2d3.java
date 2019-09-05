hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/localizer/ResourceLocalizationService.java
  }

  private void cleanUpLocalDirs(FileContext lfs, DeletionService del) {
    for (String localDir : dirsHandler.getLocalDirsForCleanup()) {
      cleanUpLocalDir(lfs, del, localDir);
    }
  }

