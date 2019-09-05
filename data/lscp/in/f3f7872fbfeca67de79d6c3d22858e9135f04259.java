hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/util/hash/JenkinsHash.java
      System.err.println("Usage: JenkinsHash filename");
      System.exit(-1);
    }
    try (FileInputStream in = new FileInputStream(args[0])) {
      byte[] bytes = new byte[512];
      int value = 0;
      JenkinsHash hash = new JenkinsHash();
      System.out.println(Math.abs(value));
    }
  }
}

