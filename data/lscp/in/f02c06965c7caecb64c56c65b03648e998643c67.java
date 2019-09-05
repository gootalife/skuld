hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/fs/FileSystem.java


hadoop-common-project/hadoop-common/src/test/java/org/apache/hadoop/fs/FileSystemContractBaseTest.java

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;

import junit.framework.TestCase;


    paths = fs.listStatus(path("/test/hadoop"));
    assertEquals(3, paths.length);
    ArrayList<String> list = new ArrayList<String>();
    for (FileStatus fileState : paths) {
      list.add(fileState.getPath().toString());
    }
    assertTrue(list.contains(path("/test/hadoop/a")));
    assertTrue(list.contains(path("/test/hadoop/b")));
    assertTrue(list.contains(path("/test/hadoop/c")));

    paths = fs.listStatus(path("/test/hadoop/a"));
    assertEquals(0, paths.length);

