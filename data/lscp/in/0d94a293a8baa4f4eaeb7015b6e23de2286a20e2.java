hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/fs/shell/Mkdir.java
  protected void processNonexistentPath(PathData item) throws IOException {
    if (!createParents &&
        !item.fs.exists(new Path(item.path.toString()).getParent())) {
      throw new PathNotFoundException(item.toString());
    }
    if (!item.fs.mkdirs(item.path)) {

