hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/util/RunJar.java
    String fileName = args[firstArg++];
    File file = new File(fileName);
    if (!file.exists() || !file.isFile()) {
      System.err.println("JAR does not exist or is not a normal file: " +
          file.getCanonicalPath());
      System.exit(-1);
    }
    String mainClassName = null;

