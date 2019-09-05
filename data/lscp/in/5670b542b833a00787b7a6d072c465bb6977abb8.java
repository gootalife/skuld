hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/crypto/random/OsSecureRandom.java
  private void fillReservoir(int min) {
    if (pos >= reservoir.length - min) {
      try {
        if (stream == null) {
          stream = new FileInputStream(new File(randomDevPath));
        }
        IOUtils.readFully(stream, reservoir, 0, reservoir.length);
      } catch (IOException e) {
        throw new RuntimeException("failed to fill reservoir", e);
    this.randomDevPath = conf.get(
        HADOOP_SECURITY_SECURE_RANDOM_DEVICE_FILE_PATH_KEY,
        HADOOP_SECURITY_SECURE_RANDOM_DEVICE_FILE_PATH_DEFAULT);
    close();
  }

  @Override

