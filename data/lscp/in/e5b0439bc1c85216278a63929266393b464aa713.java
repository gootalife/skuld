hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/java/org/apache/hadoop/mapreduce/JobResourceUploader.java

    FileSystem remoteFs = null;
    remoteFs = originalPath.getFileSystem(conf);
    if (FileUtil.compareFs(remoteFs, jtFs)) {
      return originalPath;
    }
    return newPath;
  }

  private void copyJar(Path originalJarPath, Path submitJarFile,
      short replication) throws IOException {
    jtFs.copyFromLocalFile(originalJarPath, submitJarFile);

