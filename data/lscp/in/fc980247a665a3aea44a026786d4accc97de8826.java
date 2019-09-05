hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/test/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/localizer/TestResourceLocalizationService.java
  @After
  public void cleanup() throws IOException {
    conf = null;
    try {
      FileUtils.deleteDirectory(new File(basedir.toString()));
    } catch (IOException e) {
    }
  }
  
  @Test

