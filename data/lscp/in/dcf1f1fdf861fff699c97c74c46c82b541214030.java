hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestBlockStoragePolicy.java
  static final EnumSet<StorageType> none = EnumSet.noneOf(StorageType.class);
  static final EnumSet<StorageType> archive = EnumSet.of(StorageType.ARCHIVE);
  static final EnumSet<StorageType> disk = EnumSet.of(StorageType.DISK);
  static final EnumSet<StorageType> ssd = EnumSet.of(StorageType.SSD);
  static final EnumSet<StorageType> disk_archive = EnumSet.of(StorageType.DISK,
      StorageType.ARCHIVE);
  static final EnumSet<StorageType> all = EnumSet.of(StorageType.SSD,
      StorageType.DISK, StorageType.ARCHIVE);

  static final long FILE_LEN = 1024;
  static final short REPLICATION = 3;
        final List<StorageType> computed = cold.chooseStorageTypes(replication);
        assertStorageType(computed, replication, StorageType.ARCHIVE);
      }
      assertCreationFallback(cold, null, null, null, null, null);
      assertReplicationFallback(cold, null, null, null, null);
    }
    
    { // check Warm policy
        final List<StorageType> computed = warm.chooseStorageTypes(replication);
        assertStorageType(computed, replication, StorageType.DISK, StorageType.ARCHIVE);
      }
      assertCreationFallback(warm, StorageType.DISK, StorageType.DISK,
          StorageType.ARCHIVE, StorageType.DISK, null);
      assertReplicationFallback(warm, StorageType.DISK, StorageType.DISK,
          StorageType.ARCHIVE, StorageType.DISK);
    }

    { // check Hot policy
        final List<StorageType> computed = hot.chooseStorageTypes(replication);
        assertStorageType(computed, replication, StorageType.DISK);
      }
      assertCreationFallback(hot, null, null, null, null, null);
      assertReplicationFallback(hot, StorageType.ARCHIVE, null,
          StorageType.ARCHIVE, StorageType.ARCHIVE);
    }

    { // check ONE_SSD policy
      final BlockStoragePolicy onessd = POLICY_SUITE.getPolicy(ONESSD);
      for (short replication = 1; replication < 6; replication++) {
        final List<StorageType> computed = onessd
            .chooseStorageTypes(replication);
        assertStorageType(computed, replication, StorageType.SSD,
            StorageType.DISK);
      }
      assertCreationFallback(onessd, StorageType.SSD, StorageType.SSD,
          StorageType.SSD, StorageType.DISK, StorageType.SSD);
      assertReplicationFallback(onessd, StorageType.SSD, StorageType.SSD,
          StorageType.SSD, StorageType.DISK);
    }

    { // check ALL_SSD policy
      final BlockStoragePolicy allssd = POLICY_SUITE.getPolicy(ALLSSD);
      for (short replication = 1; replication < 6; replication++) {
        final List<StorageType> computed = allssd
            .chooseStorageTypes(replication);
        assertStorageType(computed, replication, StorageType.SSD);
      }
      assertCreationFallback(allssd, StorageType.DISK, StorageType.DISK, null,
          StorageType.DISK, null);
      assertReplicationFallback(allssd, StorageType.DISK, StorageType.DISK,
          null, StorageType.DISK);
    }

    { // check LAZY_PERSIST policy
      final BlockStoragePolicy lazyPersist = POLICY_SUITE
          .getPolicy(LAZY_PERSIST);
      for (short replication = 1; replication < 6; replication++) {
        final List<StorageType> computed = lazyPersist
            .chooseStorageTypes(replication);
        assertStorageType(computed, replication, StorageType.DISK);
      }
      assertCreationFallback(lazyPersist, StorageType.DISK, StorageType.DISK,
          null, StorageType.DISK, null);
      assertReplicationFallback(lazyPersist, StorageType.DISK,
          StorageType.DISK, null, StorageType.DISK);
    }
  }

    }
  }

  static void assertCreationFallback(BlockStoragePolicy policy,
      StorageType noneExpected, StorageType archiveExpected,
      StorageType diskExpected, StorageType ssdExpected,
      StorageType disk_archiveExpected) {
    Assert.assertEquals(noneExpected, policy.getCreationFallback(none));
    Assert.assertEquals(archiveExpected, policy.getCreationFallback(archive));
    Assert.assertEquals(diskExpected, policy.getCreationFallback(disk));
    Assert.assertEquals(ssdExpected, policy.getCreationFallback(ssd));
    Assert.assertEquals(disk_archiveExpected,
        policy.getCreationFallback(disk_archive));
    Assert.assertEquals(null, policy.getCreationFallback(all));
  }

  static void assertReplicationFallback(BlockStoragePolicy policy,
      StorageType noneExpected, StorageType archiveExpected,
      StorageType diskExpected, StorageType ssdExpected) {
    Assert.assertEquals(noneExpected, policy.getReplicationFallback(none));
    Assert
        .assertEquals(archiveExpected, policy.getReplicationFallback(archive));
    Assert.assertEquals(diskExpected, policy.getReplicationFallback(disk));
    Assert.assertEquals(ssdExpected, policy.getReplicationFallback(ssd));
    Assert.assertEquals(null, policy.getReplicationFallback(all));
  }

  private static interface CheckChooseStorageTypes {
      public void checkChooseStorageTypes(BlockStoragePolicy p,
          short replication, List<StorageType> chosen, StorageType... expected) {
        final List<StorageType> types = p.chooseStorageTypes(replication,
            chosen, disk_archive, true);
        assertStorageTypes(types, expected);
      }
    };
      public void checkChooseStorageTypes(BlockStoragePolicy p,
          short replication, List<StorageType> chosen, StorageType... expected) {
        final List<StorageType> types = p.chooseStorageTypes(replication,
            chosen, disk_archive, false);
        assertStorageTypes(types, expected);
      }
    };

