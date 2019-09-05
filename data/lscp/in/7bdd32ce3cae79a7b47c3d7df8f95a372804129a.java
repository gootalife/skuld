hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/inotify/Event.java
  @InterfaceAudience.Public
  public static class CloseEvent extends Event {
    private String path;
    private long fileSize;
    public long getTimestamp() {
      return timestamp;
    }

    @Override
    @InterfaceStability.Unstable
    public String toString() {
      return "CloseEvent [path=" + path + ", fileSize=" + fileSize
          + ", timestamp=" + timestamp + "]";
    }

  }

  @InterfaceAudience.Public
  public static class CreateEvent extends Event {

    public static enum INodeType {
    public long getDefaultBlockSize() {
      return defaultBlockSize;
    }

    @Override
    @InterfaceStability.Unstable
    public String toString() {
      StringBuilder content = new StringBuilder();
      content.append("CreateEvent [INodeType=" + iNodeType + ", path=" + path
          + ", ctime=" + ctime + ", replication=" + replication
          + ", ownerName=" + ownerName + ", groupName=" + groupName
          + ", perms=" + perms + ", ");

      if (symlinkTarget != null) {
        content.append("symlinkTarget=" + symlinkTarget + ", ");
      }

      content.append("overwrite=" + overwrite + ", defaultBlockSize="
          + defaultBlockSize + "]");
      return content.toString();
    }

  }

  @InterfaceAudience.Public
  public static class MetadataUpdateEvent extends Event {

    public static enum MetadataType {
      return xAttrsRemoved;
    }

    @Override
    @InterfaceStability.Unstable
    public String toString() {
      StringBuilder content = new StringBuilder();
      content.append("MetadataUpdateEvent [path=" + path + ", metadataType="
          + metadataType);
      switch (metadataType) {
      case TIMES:
        content.append(", mtime=" + mtime + ", atime=" + atime);
        break;
      case REPLICATION:
        content.append(", replication=" + replication);
        break;
      case OWNER:
        content.append(", ownerName=" + ownerName
            + ", groupName=" + groupName);
        break;
      case PERMS:
        content.append(", perms=" + perms);
        break;
      case ACLS:
        content.append(", acls=" + acls);
        break;
      case XATTRS:
        content.append(", xAttrs=" + xAttrs + ", xAttrsRemoved="
            + xAttrsRemoved);
        break;
      default:
        break;
      }
      content.append(']');
      return content.toString();
    }
  }

  @InterfaceAudience.Public
  public static class RenameEvent extends Event {
    private String srcPath;
    private String dstPath;
    public long getTimestamp() {
      return timestamp;
    }

    @Override
    @InterfaceStability.Unstable
    public String toString() {
      return "RenameEvent [srcPath=" + srcPath + ", dstPath=" + dstPath
          + ", timestamp=" + timestamp + "]";
    }

  }

  @InterfaceAudience.Public
  public static class AppendEvent extends Event {
    private String path;
    private boolean newBlock;
    public boolean toNewBlock() {
      return newBlock;
    }

    @Override
    @InterfaceStability.Unstable
    public String toString() {
      return "AppendEvent [path=" + path + ", newBlock=" + newBlock + "]";
    }

  }

  @InterfaceAudience.Public
  public static class UnlinkEvent extends Event {
    private String path;
    private long timestamp;
    public long getTimestamp() {
      return timestamp;
    }

    @Override
    @InterfaceStability.Unstable
    public String toString() {
      return "UnlinkEvent [path=" + path + ", timestamp=" + timestamp + "]";
    }
  }

  @InterfaceAudience.Public
  public static class TruncateEvent extends Event {
    private String path;
    private long fileSize;
    public long getTimestamp() {
      return timestamp;
    }

    @Override
    @InterfaceStability.Unstable
    public String toString() {
      return "TruncateEvent [path=" + path + ", fileSize=" + fileSize
          + ", timestamp=" + timestamp + "]";
    }
  }
}

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestDFSInotifyEventInputStream.java
      Assert.assertEquals("/file4", re.getDstPath());
      Assert.assertEquals("/file", re.getSrcPath());
      Assert.assertTrue(re.getTimestamp() > 0);
      LOG.info(re.toString());
      Assert.assertTrue(re.toString().startsWith("RenameEvent [srcPath="));

      long eventsBehind = eis.getTxidsBehindEstimate();

      Assert.assertTrue(re2.getDstPath().equals("/file2"));
      Assert.assertTrue(re2.getSrcPath().equals("/file4"));
      Assert.assertTrue(re.getTimestamp() > 0);
      LOG.info(re2.toString());

      batch = waitForNextEvents(eis);
      Assert.assertTrue(ce.getSymlinkTarget() == null);
      Assert.assertTrue(ce.getOverwrite());
      Assert.assertEquals(BLOCK_SIZE, ce.getDefaultBlockSize());
      LOG.info(ce.toString());
      Assert.assertTrue(ce.toString().startsWith("CreateEvent [INodeType="));

      batch = waitForNextEvents(eis);
      Assert.assertTrue(ce2.getPath().equals("/file2"));
      Assert.assertTrue(ce2.getFileSize() > 0);
      Assert.assertTrue(ce2.getTimestamp() > 0);
      LOG.info(ce2.toString());
      Assert.assertTrue(ce2.toString().startsWith("CloseEvent [path="));

      batch = waitForNextEvents(eis);
      Event.AppendEvent append2 = (Event.AppendEvent)batch.getEvents()[0];
      Assert.assertEquals("/file2", append2.getPath());
      Assert.assertFalse(append2.toNewBlock());
      LOG.info(append2.toString());
      Assert.assertTrue(append2.toString().startsWith("AppendEvent [path="));

      batch = waitForNextEvents(eis);
      Assert.assertTrue(mue.getPath().equals("/file2"));
      Assert.assertTrue(mue.getMetadataType() ==
          Event.MetadataUpdateEvent.MetadataType.TIMES);
      LOG.info(mue.toString());
      Assert.assertTrue(mue.toString().startsWith("MetadataUpdateEvent [path="));

      batch = waitForNextEvents(eis);
      Assert.assertTrue(mue2.getMetadataType() ==
          Event.MetadataUpdateEvent.MetadataType.REPLICATION);
      Assert.assertTrue(mue2.getReplication() == 1);
      LOG.info(mue2.toString());

      batch = waitForNextEvents(eis);
      Event.UnlinkEvent ue2 = (Event.UnlinkEvent) batch.getEvents()[1];
      Assert.assertTrue(ue2.getPath().equals("/file3"));
      Assert.assertTrue(ue2.getTimestamp() > 0);
      LOG.info(ue2.toString());
      Assert.assertTrue(ue2.toString().startsWith("UnlinkEvent [path="));
      Assert.assertTrue(batch.getEvents()[2].getEventType() == Event.EventType.CLOSE);
      Event.CloseEvent ce3 = (Event.CloseEvent) batch.getEvents()[2];
      Assert.assertTrue(ce3.getPath().equals("/file2"));
      Event.UnlinkEvent ue = (Event.UnlinkEvent) batch.getEvents()[0];
      Assert.assertTrue(ue.getPath().equals("/file2"));
      Assert.assertTrue(ue.getTimestamp() > 0);
      LOG.info(ue.toString());

      batch = waitForNextEvents(eis);
      Assert.assertTrue(ce4.getCtime() > 0);
      Assert.assertTrue(ce4.getReplication() == 0);
      Assert.assertTrue(ce4.getSymlinkTarget() == null);
      LOG.info(ce4.toString());

      batch = waitForNextEvents(eis);
      Assert.assertTrue(mue3.getMetadataType() ==
          Event.MetadataUpdateEvent.MetadataType.PERMS);
      Assert.assertTrue(mue3.getPerms().toString().contains("rw-rw-rw-"));
      LOG.info(mue3.toString());

      batch = waitForNextEvents(eis);
          Event.MetadataUpdateEvent.MetadataType.OWNER);
      Assert.assertTrue(mue4.getOwnerName().equals("username"));
      Assert.assertTrue(mue4.getGroupName().equals("groupname"));
      LOG.info(mue4.toString());

      batch = waitForNextEvents(eis);
      Assert.assertTrue(ce5.getCtime() > 0);
      Assert.assertTrue(ce5.getReplication() == 0);
      Assert.assertTrue(ce5.getSymlinkTarget().equals("/dir"));
      LOG.info(ce5.toString());

      batch = waitForNextEvents(eis);
      Assert.assertTrue(mue5.getxAttrs().size() == 1);
      Assert.assertTrue(mue5.getxAttrs().get(0).getName().contains("field"));
      Assert.assertTrue(!mue5.isxAttrsRemoved());
      LOG.info(mue5.toString());

      batch = waitForNextEvents(eis);
      Assert.assertTrue(mue6.getxAttrs().size() == 1);
      Assert.assertTrue(mue6.getxAttrs().get(0).getName().contains("field"));
      Assert.assertTrue(mue6.isxAttrsRemoved());
      LOG.info(mue6.toString());

      batch = waitForNextEvents(eis);
          Event.MetadataUpdateEvent.MetadataType.ACLS);
      Assert.assertTrue(mue7.getAcls().contains(
          AclEntry.parseAclEntry("user::rwx", true)));
      LOG.info(mue7.toString());

      batch = waitForNextEvents(eis);
      Assert.assertTrue(mue8.getMetadataType() ==
          Event.MetadataUpdateEvent.MetadataType.ACLS);
      Assert.assertTrue(mue8.getAcls() == null);
      LOG.info(mue8.toString());

      batch = waitForNextEvents(eis);
      Assert.assertTrue(re3.getDstPath().equals("/dir/file5"));
      Assert.assertTrue(re3.getSrcPath().equals("/file5"));
      Assert.assertTrue(re.getTimestamp() > 0);
      LOG.info(re3.toString());

      batch = waitForNextEvents(eis);
      Assert.assertTrue(et.getPath().equals("/truncate_file"));
      Assert.assertTrue(et.getFileSize() == BLOCK_SIZE);
      Assert.assertTrue(et.getTimestamp() > 0);
      LOG.info(et.toString());
      Assert.assertTrue(et.toString().startsWith("TruncateEvent [path="));

      Assert.assertTrue(eis.poll() == null);

