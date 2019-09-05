hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/security/UserGroupInformation.java
  private static final boolean windows =
      System.getProperty("os.name").startsWith("Windows");
  private static final boolean is64Bit =
      System.getProperty("os.arch").contains("64") ||
      System.getProperty("os.arch").contains("s390x");
  private static final boolean aix = System.getProperty("os.name").equals("AIX");


