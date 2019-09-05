hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/fs/permission/FsPermission.java
    public ImmutableFsPermission(short permission) {
      super(permission);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      throw new UnsupportedOperationException();

