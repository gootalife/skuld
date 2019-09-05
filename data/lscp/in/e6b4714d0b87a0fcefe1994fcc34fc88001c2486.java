hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/fs/permission/UmaskParser.java
@InterfaceStability.Unstable
class UmaskParser extends PermissionParser {
  private static Pattern chmodOctalPattern =
    Pattern.compile("^\\s*[+]?(0*)([0-7]{3})\\s*$"); // no leading 1 for sticky bit
  private static Pattern umaskSymbolicPattern =    /* not allow X or t */
    Pattern.compile("\\G\\s*([ugoa]*)([+=-]+)([rwx]*)([,\\s]*)\\s*");
  final short umaskMode;

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/security/TestPermission.java
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Random;
    conf = new Configuration();
    conf.set(FsPermission.UMASK_LABEL, "022");
    assertEquals(18, FsPermission.getUMask(conf).toShort());

    conf = new Configuration();
    conf.set(FsPermission.UMASK_LABEL, "0022");
    assertEquals(18, FsPermission.getUMask(conf).toShort());

    conf = new Configuration();
    conf.set(FsPermission.UMASK_LABEL, "1222");
    try {
      FsPermission.getUMask(conf);
      fail("expect IllegalArgumentException happen");
    } catch (IllegalArgumentException e) {
    }

    conf = new Configuration();
    conf.set(FsPermission.UMASK_LABEL, "01222");
    try {
      FsPermission.getUMask(conf);
      fail("expect IllegalArgumentException happen");
    } catch (IllegalArgumentException e) {
    }
  }

  @Test

