hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/fs/RawLocalFileSystem.java
    if (!exists(f)) {
      throw new FileNotFoundException("File " + f + " not found");
    }
    FileStatus status = getFileStatus(f);
    if (status.isDirectory()) {
      throw new IOException("Cannot append to a diretory (=" + f + " )");
    }
    return new FSDataOutputStream(new BufferedOutputStream(
        createOutputStreamWithMode(f, true, null), bufferSize), statistics,
        status.getLen());
  }

  @Override

hadoop-common-project/hadoop-common/src/test/java/org/apache/hadoop/fs/TestLocalFileSystem.java
    assertEquals("resolvePath did not strip fragment from Path", pathQualified,
        resolved);
  }

  @Test
  public void testAppendSetsPosCorrectly() throws Exception {
    FileSystem fs = fileSys.getRawFileSystem();
    Path file = new Path(TEST_ROOT_DIR, "test-append");

    fs.delete(file, true);
    FSDataOutputStream out = fs.create(file);

    try {
      out.write("text1".getBytes());
    } finally {
      out.close();
    }

    out = fs.append(file);
    try {
      assertEquals(5, out.getPos());
      out.write("text2".getBytes());
    } finally {
      out.close();
    }

    FSDataInputStream in = fs.open(file);
    try {
      byte[] buf = new byte[in.available()];
      in.readFully(buf);
      assertEquals("text1text2", new String(buf));
    } finally {
      in.close();
    }
  }

}

