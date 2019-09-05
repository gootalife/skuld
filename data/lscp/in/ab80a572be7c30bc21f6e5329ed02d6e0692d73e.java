hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/BlockInfoContiguous.java
  protected BlockInfoContiguous(BlockInfoContiguous from) {
    super(from);
    this.triplets = new Object[from.triplets.length];
    this.bc = from.bc;
  }


hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/blockmanagement/TestBlockInfo.java
    Assert.assertEquals(storage, blockInfo.getStorageInfo(0));
  }

  @Test
  public void testCopyConstructor() {
    BlockInfoContiguous old = new BlockInfoContiguous((short) 3);
    try {
      BlockInfoContiguous copy = new BlockInfoContiguous(old);
      assertEquals(old.getBlockCollection(), copy.getBlockCollection());
      assertEquals(old.getCapacity(), copy.getCapacity());
    } catch (Exception e) {
      Assert.fail("Copy constructor throws exception: " + e);
    }
  }

  @Test
  public void testReplaceStorage() throws Exception {

