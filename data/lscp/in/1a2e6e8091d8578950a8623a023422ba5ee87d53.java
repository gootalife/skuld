hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/java/org/apache/hadoop/mapred/Task.java
    out.writeBoolean(taskCleanup);
    Text.writeString(out, user);
    out.writeInt(encryptedSpillKey.length);
    extraData.write(out);
    out.write(encryptedSpillKey);
  }
  
  public void readFields(DataInput in) throws IOException {
    user = StringInterner.weakIntern(Text.readString(in));
    int len = in.readInt();
    encryptedSpillKey = new byte[len];
    extraData.readFields(in);
    in.readFully(encryptedSpillKey);
  }

  @Override

