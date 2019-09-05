hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestReplication.java
package org.apache.hadoop.hdfs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.net.InetSocketAddress;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;
        if (data_dir.listFiles().length == 0) {
          nonParticipatedNodeDirs.add(data_dir);
        } else {
          assertNull("participatedNodeDirs has already been set.",
              participatedNodeDirs);
          participatedNodeDirs = data_dir;
        }
      }
      assertEquals(2, nonParticipatedNodeDirs.size());

      String blockFile = null;
      final List<File> listFiles = new ArrayList<>();
      Files.walkFileTree(participatedNodeDirs.toPath(),
          new SimpleFileVisitor<java.nio.file.Path>() {
            @Override
            public FileVisitResult visitFile(
                java.nio.file.Path file, BasicFileAttributes attrs)
                throws IOException {
              listFiles.add(file.toFile());
              return FileVisitResult.CONTINUE;
            }
          }
      );
      assertFalse(listFiles.isEmpty());
      int numReplicaCreated = 0;
      for (File file : listFiles) {
        if (file.getName().startsWith(Block.BLOCK_FILE_PREFIX)
            && !file.getName().endsWith("meta")) {
            file1.mkdirs();
            new File(file1, blockFile).createNewFile();
            new File(file1, blockFile + "_1000.meta").createNewFile();
            numReplicaCreated++;
          }
          break;
        }
      }
      assertEquals(2, numReplicaCreated);

      fs.setReplication(new Path("/test"), (short) 3);
      cluster.restartDataNodes(); // Lets detect all DNs about dummy copied

