hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSDirRenameOp.java
      fsd.writeUnlock();
    }
    if (stat) {
      fsd.getEditLog().logRename(src, actualDst, mtime, logRetryCache);
      return true;
    }
    return false;

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSEditLog.java
  }
  
  void logRename(String src, String dst, long timestamp, boolean toLogRpcIds) {
  }

  void logRename(String src, String dst, long timestamp, boolean toLogRpcIds,
      Options.Rename... options) {

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestDFSInotifyEventInputStream.java
      client.setAcl("/file5", AclEntry.parseAclSpec(
          "user::rwx,user:foo:rw-,group::r--,other::---", true));
      client.removeAcl("/file5"); // SetAclOp -> MetadataUpdateEvent
      client.rename("/file5", "/dir"); // RenameOldOp -> RenameEvent

      EventBatch batch = null;

          Event.MetadataUpdateEvent.MetadataType.ACLS);
      Assert.assertTrue(mue8.getAcls() == null);

      batch = waitForNextEvents(eis);
      Assert.assertEquals(1, batch.getEvents().length);
      txid = checkTxid(batch, txid);
      Assert.assertTrue(batch.getEvents()[0].getEventType() == Event.EventType.RENAME);
      Event.RenameEvent re3 = (Event.RenameEvent) batch.getEvents()[0];
      Assert.assertTrue(re3.getDstPath().equals("/dir/file5"));
      Assert.assertTrue(re3.getSrcPath().equals("/file5"));
      Assert.assertTrue(re.getTimestamp() > 0);

      Assert.assertTrue(eis.poll() == null);


