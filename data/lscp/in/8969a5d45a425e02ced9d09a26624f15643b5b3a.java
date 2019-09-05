hadoop-common-project/hadoop-common/src/test/java/org/apache/hadoop/fs/TestFsShellCopy.java
      checkPut(0, srcPath, dstPath, useWindowsPath);
    }

    prepPut(dstPath, false, false);
    checkPut(1, srcPath, childPath, useWindowsPath);

    prepPut(dstPath, true, true);

