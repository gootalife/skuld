hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSNamesystem.java
  public int getEffectiveLayoutVersion() {
    return getEffectiveLayoutVersion(isRollingUpgrade(),
        fsImage.getStorage().getLayoutVersion(),
        NameNodeLayoutVersion.MINIMUM_COMPATIBLE_LAYOUT_VERSION,
        NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION);
  }

  @VisibleForTesting
  static int getEffectiveLayoutVersion(boolean isRollingUpgrade, int storageLV,
      int minCompatLV, int currentLV) {
    if (isRollingUpgrade) {
      if (storageLV <= minCompatLV) {
    }
    return currentLV;
  }


hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/TestFSNamesystem.java
    fsn.imageLoadComplete();
    assertTrue(fsn.isImageLoaded());
  }

  @Test
  public void testGetEffectiveLayoutVersion() {
    assertEquals(-63,
        FSNamesystem.getEffectiveLayoutVersion(true, -60, -61, -63));
    assertEquals(-61,
        FSNamesystem.getEffectiveLayoutVersion(true, -61, -61, -63));
    assertEquals(-62,
        FSNamesystem.getEffectiveLayoutVersion(true, -62, -61, -63));
    assertEquals(-63,
        FSNamesystem.getEffectiveLayoutVersion(true, -63, -61, -63));
    assertEquals(-63,
        FSNamesystem.getEffectiveLayoutVersion(false, -60, -61, -63));
    assertEquals(-63,
        FSNamesystem.getEffectiveLayoutVersion(false, -61, -61, -63));
    assertEquals(-63,
        FSNamesystem.getEffectiveLayoutVersion(false, -62, -61, -63));
    assertEquals(-63,
        FSNamesystem.getEffectiveLayoutVersion(false, -63, -61, -63));
  }
}

