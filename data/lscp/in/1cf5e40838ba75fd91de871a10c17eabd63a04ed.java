hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestDFSInotifyEventInputStream.java
      Event.RenameEvent re2 = (Event.RenameEvent) batch.getEvents()[0];
      Assert.assertTrue(re2.getDstPath().equals("/file2"));
      Assert.assertTrue(re2.getSrcPath().equals("/file4"));
      Assert.assertTrue(re2.getTimestamp() > 0);
      LOG.info(re2.toString());

      Event.RenameEvent re3 = (Event.RenameEvent) batch.getEvents()[0];
      Assert.assertTrue(re3.getDstPath().equals("/dir/file5"));
      Assert.assertTrue(re3.getSrcPath().equals("/file5"));
      Assert.assertTrue(re3.getTimestamp() > 0);
      LOG.info(re3.toString());


