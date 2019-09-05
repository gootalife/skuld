hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/inotify/Event.java
@InterfaceStability.Unstable
public abstract class Event {
  public static enum EventType {
    CREATE, CLOSE, APPEND, RENAME, METADATA, UNLINK, TRUNCATE
  }

  private EventType eventType;
      return timestamp;
    }
  }

  public static class TruncateEvent extends Event {
    private String path;
    private long fileSize;
    private long timestamp;


    public TruncateEvent(String path, long fileSize, long timestamp) {
      super(EventType.TRUNCATE);
      this.path = path;
      this.fileSize = fileSize;
      this.timestamp = timestamp;
    }

    public String getPath() {
      return path;
    }

    public long getFileSize() {
      return fileSize;
    }

    public long getTimestamp() {
      return timestamp;
    }
  }
}

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/protocolPB/PBHelper.java
                  .timestamp(unlink.getTimestamp())
                  .build());
            break;
          case EVENT_TRUNCATE:
            InotifyProtos.TruncateEventProto truncate =
                InotifyProtos.TruncateEventProto.parseFrom(p.getContents());
            events.add(new Event.TruncateEvent(truncate.getPath(),
                truncate.getFileSize(), truncate.getTimestamp()));
            break;
          default:
            throw new RuntimeException("Unexpected inotify event type: " +
                p.getType());
                        .setTimestamp(ue.getTimestamp()).build().toByteString()
                ).build());
            break;
          case TRUNCATE:
            Event.TruncateEvent te = (Event.TruncateEvent) e;
            events.add(InotifyProtos.EventProto.newBuilder()
                .setType(InotifyProtos.EventType.EVENT_TRUNCATE)
                .setContents(
                    InotifyProtos.TruncateEventProto.newBuilder()
                        .setPath(te.getPath())
                        .setFileSize(te.getFileSize())
                        .setTimestamp(te.getTimestamp()).build().toByteString()
                ).build());
            break;
          default:
            throw new RuntimeException("Unexpected inotify event: " + e);
        }

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/InotifyFSEditLogOpTranslator.java
          .metadataType(Event.MetadataUpdateEvent.MetadataType.ACLS)
          .path(saOp.src)
          .acls(saOp.aclEntries).build() });
    case OP_TRUNCATE:
      FSEditLogOp.TruncateOp tOp = (FSEditLogOp.TruncateOp) op;
      return new EventBatch(op.txid, new Event[] {
          new Event.TruncateEvent(tOp.src, tOp.newLength, tOp.timestamp) });
    default:
      return null;
    }

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestDFSInotifyEventInputStream.java
      DFSTestUtil.createFile(fs, new Path("/file"), BLOCK_SIZE, (short) 1, 0L);
      DFSTestUtil.createFile(fs, new Path("/file3"), BLOCK_SIZE, (short) 1, 0L);
      DFSTestUtil.createFile(fs, new Path("/file5"), BLOCK_SIZE, (short) 1, 0L);
      DFSTestUtil.createFile(fs, new Path("/truncate_file"),
          BLOCK_SIZE * 2, (short) 1, 0L);
      DFSInotifyEventInputStream eis = client.getInotifyEventStream();
      client.rename("/file", "/file4", null); // RenameOp -> RenameEvent
      client.rename("/file4", "/file2"); // RenameOldOp -> RenameEvent
          "user::rwx,user:foo:rw-,group::r--,other::---", true));
      client.removeAcl("/file5"); // SetAclOp -> MetadataUpdateEvent
      client.rename("/file5", "/dir"); // RenameOldOp -> RenameEvent
      client.truncate("/truncate_file", BLOCK_SIZE);
      EventBatch batch = null;

      Assert.assertTrue(re3.getSrcPath().equals("/file5"));
      Assert.assertTrue(re.getTimestamp() > 0);

      batch = waitForNextEvents(eis);
      Assert.assertEquals(1, batch.getEvents().length);
      txid = checkTxid(batch, txid);
      Assert
          .assertTrue(batch.getEvents()[0].getEventType() ==
          Event.EventType.TRUNCATE);
      Event.TruncateEvent et = ((Event.TruncateEvent) batch.getEvents()[0]);
      Assert.assertTrue(et.getPath().equals("/truncate_file"));
      Assert.assertTrue(et.getFileSize() == BLOCK_SIZE);
      Assert.assertTrue(et.getTimestamp() > 0);

      Assert.assertTrue(eis.poll() == null);


