hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/fs/shell/Delete.java

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.PathIsDirectoryException;
import org.apache.hadoop.fs.PathIsNotDirectoryException;
    @Override
    protected void processArguments(LinkedList<PathData> args)
    throws IOException {
      FileSystem[] childFileSystems =
          FileSystem.get(getConf()).getChildFileSystems();
      if (null != childFileSystems) {
        for (FileSystem fs : childFileSystems) {
          Trash trash = new Trash(fs, getConf());
          trash.expunge();
          trash.checkpoint();
        }
      } else {
        Trash trash = new Trash(getConf());
        trash.expunge();
        trash.checkpoint();
      }
    }
  }
}

hadoop-common-project/hadoop-common/src/test/java/org/apache/hadoop/fs/TestTrash.java
    TestLFS() {
      this(new Path(TEST_DIR, "user/test"));
    }
    TestLFS(final Path home) {
      super(new RawLocalFileSystem() {
        @Override
        protected Path getInitialWorkingDirectory() {
          return makeQualified(home);
        }

        @Override
        public Path getHomeDirectory() {
          return makeQualified(home);
        }
      });
      this.home = home;
    }
    @Override

