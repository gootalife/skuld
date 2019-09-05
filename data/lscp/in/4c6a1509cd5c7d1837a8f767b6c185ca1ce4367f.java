hadoop-common-project/hadoop-common/src/test/java/org/apache/hadoop/fs/shell/TestCount.java
    count.processOptions(options);
    String withStorageTypeHeader =
        "    SSD_QUOTA     REM_SSD_QUOTA    DISK_QUOTA    REM_DISK_QUOTA " +
        "ARCHIVE_QUOTA REM_ARCHIVE_QUOTA " +
        "PATHNAME";
    count.processOptions(options);
    String withStorageTypeHeader =
        "    SSD_QUOTA     REM_SSD_QUOTA " +
        "   DISK_QUOTA    REM_DISK_QUOTA " +
        "ARCHIVE_QUOTA REM_ARCHIVE_QUOTA " +
        "PATHNAME";
    verify(out).println(withStorageTypeHeader);

