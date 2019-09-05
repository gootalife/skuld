hadoop-common-project/hadoop-common/src/test/java/org/apache/hadoop/http/HttpServerFunctionalTest.java
        File.separatorChar + TEST);
    try {
      if (!testWebappDir.exists()) {
        if (!testWebappDir.mkdirs()) {
          fail("Test webapp dir " + testWebappDir.getCanonicalPath()
              + " can not be created");
        }
      }
    } catch (IOException e) {
    }
  }


