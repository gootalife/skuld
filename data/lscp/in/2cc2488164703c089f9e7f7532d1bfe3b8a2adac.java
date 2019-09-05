hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/util/Shell.java
    } catch (IOException ioe) {
      LOG.debug("setsid is not available on this machine. So not using it.");
      setsidSupported = false;
    }  catch (Error err) {
      if (err.getMessage().contains("posix_spawn is not " +
          "a supported process launch mechanism")
          && (Shell.FREEBSD || Shell.MAC)) {
        LOG.info("Avoiding JDK-8047340 on BSD-based systems.", err);
        setsidSupported = false;
      }
    }  finally { // handle the exit code
      if (LOG.isDebugEnabled()) {
        LOG.debug("setsid exited with exit code "

hadoop-common-project/hadoop-common/src/test/java/org/apache/hadoop/util/TestStringUtils.java

  @Test
  public void testLowerAndUpperStrings() {
    Locale defaultLocale = Locale.getDefault();
    try {
      Locale.setDefault(new Locale("tr", "TR"));

