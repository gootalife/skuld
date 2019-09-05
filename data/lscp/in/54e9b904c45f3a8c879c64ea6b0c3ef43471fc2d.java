hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/fs/shell/Touch.java
++ b/hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/fs/shell/Touch.java

package org.apache.hadoop.fs.shell;

import java.io.IOException;
import java.util.LinkedList;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.PathIsDirectoryException;
import org.apache.hadoop.fs.PathNotFoundException;

@InterfaceAudience.Private
@InterfaceStability.Unstable

class Touch extends FsCommand {
  public static void registerCommands(CommandFactory factory) {
    factory.addClass(Touchz.class, "-touchz");
  }

  public static class Touchz extends Touch {
    public static final String NAME = "touchz";
    public static final String USAGE = "<path> ...";
    public static final String DESCRIPTION =
      "Creates a file of zero length " +
      "at <path> with current time as the timestamp of that <path>. " +
      "An error is returned if the file exists with non-zero length\n";

    @Override
    protected void processOptions(LinkedList<String> args) {
      CommandFormat cf = new CommandFormat(1, Integer.MAX_VALUE);
      cf.parse(args);
    }

    @Override
    protected void processPath(PathData item) throws IOException {
      if (item.stat.isDirectory()) {
        throw new PathIsDirectoryException(item.toString());
      }
      if (item.stat.getLen() != 0) {
        throw new PathIOException(item.toString(), "Not a zero-length file");
      }
      touchz(item);
    }

    @Override
    protected void processNonexistentPath(PathData item) throws IOException {
      if (!item.parentExists()) {
        throw new PathNotFoundException(item.toString());
      }
      touchz(item);
    }

    private void touchz(PathData item) throws IOException {
      item.fs.create(item.path).close();
    }
  }
}

