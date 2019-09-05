hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/fs/FileSystem.java


hadoop-common-project/hadoop-common/src/test/java/org/apache/hadoop/fs/FileSystemContractBaseTest.java

import java.io.FileNotFoundException;
import java.io.IOException;

import junit.framework.TestCase;


    paths = fs.listStatus(path("/test/hadoop"));
    assertEquals(3, paths.length);
    assertEquals(path("/test/hadoop/a"), paths[0].getPath());
    assertEquals(path("/test/hadoop/b"), paths[1].getPath());
    assertEquals(path("/test/hadoop/c"), paths[2].getPath());

    paths = fs.listStatus(path("/test/hadoop/a"));
    assertEquals(0, paths.length);

