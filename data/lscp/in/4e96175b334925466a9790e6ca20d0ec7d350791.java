hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/java/org/apache/hadoop/mapreduce/lib/input/CombineFileInputFormat.java
import java.util.Set;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
                 maxSize, minSizeNode, minSizeRack, splits);
  }

  @VisibleForTesting
  void createSplits(Map<String, Set<OneBlockInfo>> nodeToBlocks,
                     Map<OneBlockInfo, String[]> blockToNodes,
    Set<String> completedNodes = new HashSet<String>();
    
    while(true) {
      for (Iterator<Map.Entry<String, Set<OneBlockInfo>>> iter = nodeToBlocks
          .entrySet().iterator(); iter.hasNext();) {
        Map.Entry<String, Set<OneBlockInfo>> one = iter.next();

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-jobclient/src/test/java/org/apache/hadoop/mapreduce/lib/input/TestCombineFileInputFormat.java
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat.OneBlockInfo;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat.OneFileInfo;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.google.common.collect.HashMultiset;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;

public class TestCombineFileInputFormat {

  static final int BLOCKSIZE = 1024;
  static final byte[] databuf = new byte[BLOCKSIZE];

  @Mock
  private List<String> mockList;

  @Before
  public void initMocks() {
    MockitoAnnotations.initMocks(this);
  }

  private static final String DUMMY_FS_URI = "dummyfs:///";

    assertFalse(rr.nextKeyValue());
  }

  private final class Split {
    private String name;
    private long length;
    private long offset;

    public Split(String name, long length, long offset) {
      this.name = name;
      this.length = length;
      this.offset = offset;
    }

    public String getName() {
      return name;
    }

    public long getLength() {
      return length;
    }

    public long getOffset() {
      return offset;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof Split) {
        Split split = ((Split) obj);
        return split.name.equals(name) && split.length == length
            && split.offset == offset;
      }
      return false;
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testSplitPlacement() throws Exception {
    MiniDFSCluster dfs = null;
    FileSystem fileSys = null;
      splits = inFormat.getSplits(job);
      System.out.println("Made splits(Test1): " + splits.size());

      for (InputSplit split : splits) {
        System.out.println("File split(Test1): " + split);
      }

      for (InputSplit split : splits) {
        fileSplit = (CombineFileSplit) split;
        if (splits.size() == 2) {
          if (split.equals(splits.get(0))) {
            assertEquals(2, fileSplit.getNumPaths());
            assertEquals(1, fileSplit.getLocations().length);
            assertEquals(file2.getName(), fileSplit.getPath(0).getName());
            assertEquals(file2.getName(), fileSplit.getPath(1).getName());
            assertEquals(BLOCKSIZE, fileSplit.getOffset(1));
            assertEquals(BLOCKSIZE, fileSplit.getLength(1));
            assertEquals(hosts2[0], fileSplit.getLocations()[0]);
          }
          if (split.equals(splits.get(1))) {
            assertEquals(1, fileSplit.getNumPaths());
            assertEquals(1, fileSplit.getLocations().length);
            assertEquals(file1.getName(), fileSplit.getPath(0).getName());
            assertEquals(0, fileSplit.getOffset(0));
            assertEquals(BLOCKSIZE, fileSplit.getLength(0));
            assertEquals(hosts1[0], fileSplit.getLocations()[0]);
          }
        } else if (splits.size() == 1) {
          assertEquals(3, fileSplit.getNumPaths());
          Set<Split> expected = new HashSet<>();
          expected.add(new Split(file1.getName(), BLOCKSIZE, 0));
          expected.add(new Split(file2.getName(), BLOCKSIZE, 0));
          expected.add(new Split(file2.getName(), BLOCKSIZE, BLOCKSIZE));
          List<Split> actual = new ArrayList<>();
          for (int i = 0; i < 3; i++) {
            String name = fileSplit.getPath(i).getName();
            long length = fileSplit.getLength(i);
            long offset = fileSplit.getOffset(i);
            actual.add(new Split(name, length, offset));
          }
          assertTrue(actual.containsAll(expected));
          assertEquals(1, fileSplit.getLocations().length);
          assertEquals(hosts1[0], fileSplit.getLocations()[0]);
        } else {
          fail("Expected split size is 1 or 2, but actual size is "
              + splits.size());
        }
      }

      dfs.startDataNodes(conf, 1, true, null, rack3, hosts3, null);
      for (InputSplit split : splits) {
        System.out.println("File split(Test2): " + split);
      }

      Set<Split> expected = new HashSet<>();
      expected.add(new Split(file1.getName(), BLOCKSIZE, 0));
      expected.add(new Split(file2.getName(), BLOCKSIZE, 0));
      expected.add(new Split(file2.getName(), BLOCKSIZE, BLOCKSIZE));
      expected.add(new Split(file3.getName(), BLOCKSIZE, 0));
      expected.add(new Split(file3.getName(), BLOCKSIZE, BLOCKSIZE));
      expected.add(new Split(file3.getName(), BLOCKSIZE, BLOCKSIZE * 2));
      List<Split> actual = new ArrayList<>();

      for (InputSplit split : splits) {
        fileSplit = (CombineFileSplit) split;
        if (splits.size() == 3) {
          if (split.equals(splits.get(0))) {
            assertEquals(3, fileSplit.getNumPaths());
            assertEquals(1, fileSplit.getLocations().length);
            assertEquals(file3.getName(), fileSplit.getPath(0).getName());
            assertEquals(file3.getName(), fileSplit.getPath(2).getName());
            assertEquals(2 * BLOCKSIZE, fileSplit.getOffset(2));
            assertEquals(BLOCKSIZE, fileSplit.getLength(2));
            assertEquals(hosts3[0], fileSplit.getLocations()[0]);
          }
          if (split.equals(splits.get(1))) {
            assertEquals(2, fileSplit.getNumPaths());
            assertEquals(1, fileSplit.getLocations().length);
            assertEquals(file2.getName(), fileSplit.getPath(0).getName());
            assertEquals(file2.getName(), fileSplit.getPath(1).getName());
            assertEquals(BLOCKSIZE, fileSplit.getOffset(1));
            assertEquals(BLOCKSIZE, fileSplit.getLength(1));
            assertEquals(hosts2[0], fileSplit.getLocations()[0]);
          }
          if (split.equals(splits.get(2))) {
            assertEquals(1, fileSplit.getNumPaths());
            assertEquals(1, fileSplit.getLocations().length);
            assertEquals(file1.getName(), fileSplit.getPath(0).getName());
            assertEquals(0, fileSplit.getOffset(0));
            assertEquals(BLOCKSIZE, fileSplit.getLength(0));
            assertEquals(hosts1[0], fileSplit.getLocations()[0]);
          }
        } else if (splits.size() == 2) {
          if (split.equals(splits.get(0))) {
            assertEquals(1, fileSplit.getLocations().length);
            if (fileSplit.getLocations()[0].equals(hosts2[0])) {
              assertEquals(2, fileSplit.getNumPaths());
            } else if (fileSplit.getLocations()[0].equals(hosts3[0])) {
              assertEquals(3, fileSplit.getNumPaths());
            } else {
              fail("First split should be on rack2 or rack3.");
            }
          }
          if (split.equals(splits.get(1))) {
            assertEquals(1, fileSplit.getLocations().length);
            assertEquals(hosts1[0], fileSplit.getLocations()[0]);
          }
        } else if (splits.size() == 1) {
          assertEquals(1, fileSplit.getLocations().length);
          assertEquals(6, fileSplit.getNumPaths());
          assertEquals(hosts1[0], fileSplit.getLocations()[0]);
        } else {
          fail("Split size should be 1, 2, or 3.");
        }
        for (int i = 0; i < fileSplit.getNumPaths(); i++) {
          String name = fileSplit.getPath(i).getName();
          long length = fileSplit.getLength(i);
          long offset = fileSplit.getOffset(i);
          actual.add(new Split(name, length, offset));
        }
      }

      assertEquals(6, actual.size());
      assertTrue(actual.containsAll(expected));

      Path file4 = new Path(dir4 + "/file4");
      for (InputSplit split : splits) {
        System.out.println("File split(Test3): " + split);
      }

      expected.add(new Split(file4.getName(), BLOCKSIZE, 0));
      expected.add(new Split(file4.getName(), BLOCKSIZE, BLOCKSIZE));
      expected.add(new Split(file4.getName(), BLOCKSIZE, BLOCKSIZE * 2));
      actual.clear();

      for (InputSplit split : splits) {
        fileSplit = (CombineFileSplit) split;
        if (splits.size() == 3) {
          if (split.equals(splits.get(0))) {
            assertEquals(6, fileSplit.getNumPaths());
            assertEquals(1, fileSplit.getLocations().length);
            assertEquals(hosts3[0], fileSplit.getLocations()[0]);
          }
          if (split.equals(splits.get(1))) {
            assertEquals(2, fileSplit.getNumPaths());
            assertEquals(1, fileSplit.getLocations().length);
            assertEquals(file2.getName(), fileSplit.getPath(0).getName());
            assertEquals(file2.getName(), fileSplit.getPath(1).getName());
            assertEquals(BLOCKSIZE, fileSplit.getOffset(1));
            assertEquals(BLOCKSIZE, fileSplit.getLength(1));
            assertEquals(hosts2[0], fileSplit.getLocations()[0]);
          }
          if (split.equals(splits.get(2))) {
            assertEquals(1, fileSplit.getNumPaths());
            assertEquals(1, fileSplit.getLocations().length);
            assertEquals(file1.getName(), fileSplit.getPath(0).getName());
            assertEquals(0, fileSplit.getOffset(0));
            assertEquals(BLOCKSIZE, fileSplit.getLength(0));
            assertEquals(hosts1[0], fileSplit.getLocations()[0]);
          }
        } else if (splits.size() == 2) {
          if (split.equals(splits.get(0))) {
            assertEquals(1, fileSplit.getLocations().length);
            if (fileSplit.getLocations()[0].equals(hosts2[0])) {
              assertEquals(5, fileSplit.getNumPaths());
            } else if (fileSplit.getLocations()[0].equals(hosts3[0])) {
              assertEquals(6, fileSplit.getNumPaths());
            } else {
              fail("First split should be on rack2 or rack3.");
            }
          }
          if (split.equals(splits.get(1))) {
            assertEquals(1, fileSplit.getLocations().length);
            assertEquals(hosts1[0], fileSplit.getLocations()[0]);
          }
        } else if (splits.size() == 1) {
          assertEquals(1, fileSplit.getLocations().length);
          assertEquals(9, fileSplit.getNumPaths());
          assertEquals(hosts1[0], fileSplit.getLocations()[0]);
        } else {
          fail("Split size should be 1, 2, or 3.");
        }
        for (int i = 0; i < fileSplit.getNumPaths(); i++) {
          String name = fileSplit.getPath(i).getName();
          long length = fileSplit.getLength(i);
          long offset = fileSplit.getOffset(i);
          actual.add(new Split(name, length, offset));
        }
      }

      assertEquals(9, actual.size());
      assertTrue(actual.containsAll(expected));

      inFormat = new DummyInputFormat();
        System.out.println("File split(Test4): " + split);
      }
      assertEquals(5, splits.size());

      actual.clear();
      reset(mockList);
      for (InputSplit split : splits) {
        fileSplit = (CombineFileSplit) split;
        for (int i = 0; i < fileSplit.getNumPaths(); i++) {
          String name = fileSplit.getPath(i).getName();
          long length = fileSplit.getLength(i);
          long offset = fileSplit.getOffset(i);
          actual.add(new Split(name, length, offset));
        }
        mockList.add(fileSplit.getLocations()[0]);
      }

      assertEquals(9, actual.size());
      assertTrue(actual.containsAll(expected));
      verify(mockList, atLeastOnce()).add(hosts1[0]);
      verify(mockList, atLeastOnce()).add(hosts2[0]);
      verify(mockList, atLeastOnce()).add(hosts3[0]);

      inFormat = new DummyInputFormat();
      for (InputSplit split : splits) {
        System.out.println("File split(Test5): " + split);
      }

      assertEquals(3, splits.size());

      actual.clear();
      reset(mockList);
      for (InputSplit split : splits) {
        fileSplit = (CombineFileSplit) split;
        for (int i = 0; i < fileSplit.getNumPaths(); i++) {
          String name = fileSplit.getPath(i).getName();
          long length = fileSplit.getLength(i);
          long offset = fileSplit.getOffset(i);
          actual.add(new Split(name, length, offset));
        }
        mockList.add(fileSplit.getLocations()[0]);
      }

      assertEquals(9, actual.size());
      assertTrue(actual.containsAll(expected));
      verify(mockList, atLeastOnce()).add(hosts1[0]);
      verify(mockList, atLeastOnce()).add(hosts2[0]);

      inFormat = new DummyInputFormat();
        System.out.println("File split(Test6): " + split);
      }
      assertEquals(3, splits.size());

      actual.clear();
      reset(mockList);
      for (InputSplit split : splits) {
        fileSplit = (CombineFileSplit) split;
        for (int i = 0; i < fileSplit.getNumPaths(); i++) {
          String name = fileSplit.getPath(i).getName();
          long length = fileSplit.getLength(i);
          long offset = fileSplit.getOffset(i);
          actual.add(new Split(name, length, offset));
        }
        mockList.add(fileSplit.getLocations()[0]);
      }

      assertEquals(9, actual.size());
      assertTrue(actual.containsAll(expected));
      verify(mockList, atLeastOnce()).add(hosts1[0]);

      inFormat = new DummyInputFormat();
      for (InputSplit split : splits) {
        System.out.println("File split(Test7): " + split);
      }

      assertEquals(2, splits.size());

      actual.clear();
      reset(mockList);
      for (InputSplit split : splits) {
        fileSplit = (CombineFileSplit) split;
        for (int i = 0; i < fileSplit.getNumPaths(); i++) {
          String name = fileSplit.getPath(i).getName();
          long length = fileSplit.getLength(i);
          long offset = fileSplit.getOffset(i);
          actual.add(new Split(name, length, offset));
        }
        mockList.add(fileSplit.getLocations()[0]);
      }

      assertEquals(9, actual.size());
      assertTrue(actual.containsAll(expected));
      verify(mockList, atLeastOnce()).add(hosts1[0]);

      inFormat = new DummyInputFormat();
      FileInputFormat.addInputPath(job, inDir);
      inFormat.setMinSplitSizeRack(1); // everything is at least rack local
      for (InputSplit split : splits) {
        System.out.println("File split(Test1): " + split);
      }

      for (InputSplit split : splits) {
        fileSplit = (CombineFileSplit) split;
        if (splits.size() == 2) {
          if (split.equals(splits.get(0))) {
            assertEquals(3, fileSplit.getNumPaths());
            expected.clear();
            expected.add(new Split(file1.getName(), BLOCKSIZE, 0));
            expected.add(new Split(file2.getName(), BLOCKSIZE, 0));
            expected.add(new Split(file2.getName(), BLOCKSIZE, BLOCKSIZE));
            actual.clear();
            for (int i = 0; i < 3; i++) {
              String name = fileSplit.getPath(i).getName();
              long length = fileSplit.getLength(i);
              long offset = fileSplit.getOffset(i);
              actual.add(new Split(name, length, offset));
            }
            assertTrue(actual.containsAll(expected));
            assertEquals(1, fileSplit.getLocations().length);
            assertEquals(hosts1[0], fileSplit.getLocations()[0]);
          }
          if (split.equals(splits.get(1))) {
            assertEquals(6, fileSplit.getNumPaths());
            expected.clear();
            expected.add(new Split(file3.getName(), BLOCKSIZE, 0));
            expected.add(new Split(file3.getName(), BLOCKSIZE, BLOCKSIZE));
            expected.add(new Split(file3.getName(), BLOCKSIZE, BLOCKSIZE * 2));
            expected.add(new Split(file4.getName(), BLOCKSIZE, 0));
            expected.add(new Split(file4.getName(), BLOCKSIZE, BLOCKSIZE));
            expected.add(new Split(file4.getName(), BLOCKSIZE, BLOCKSIZE * 2));
            actual.clear();
            for (int i = 0; i < 6; i++) {
              String name = fileSplit.getPath(i).getName();
              long length = fileSplit.getLength(i);
              long offset = fileSplit.getOffset(i);
              actual.add(new Split(name, length, offset));
            }
            assertTrue(actual.containsAll(expected));
            assertEquals(1, fileSplit.getLocations().length);
          }
        } else if (splits.size() == 3) {
          if (split.equals(splits.get(0))) {
            assertEquals(2, fileSplit.getNumPaths());
            expected.clear();
            expected.add(new Split(file2.getName(), BLOCKSIZE, 0));
            expected.add(new Split(file2.getName(), BLOCKSIZE, BLOCKSIZE));
            actual.clear();
            for (int i = 0; i < 2; i++) {
              String name = fileSplit.getPath(i).getName();
              long length = fileSplit.getLength(i);
              long offset = fileSplit.getOffset(i);
              actual.add(new Split(name, length, offset));
            }
            assertTrue(actual.containsAll(expected));
            assertEquals(1, fileSplit.getLocations().length);
            assertEquals(hosts2[0], fileSplit.getLocations()[0]);
          }
          if (split.equals(splits.get(1))) {
            assertEquals(1, fileSplit.getNumPaths());
            assertEquals(file1.getName(), fileSplit.getPath(0).getName());
            assertEquals(BLOCKSIZE, fileSplit.getLength(0));
            assertEquals(0, fileSplit.getOffset(0));
            assertEquals(1, fileSplit.getLocations().length);
            assertEquals(hosts1[0], fileSplit.getLocations()[0]);
          }
          if (split.equals(splits.get(2))) {
            assertEquals(6, fileSplit.getNumPaths());
            expected.clear();
            expected.add(new Split(file3.getName(), BLOCKSIZE, 0));
            expected.add(new Split(file3.getName(), BLOCKSIZE, BLOCKSIZE));
            expected.add(new Split(file3.getName(), BLOCKSIZE, BLOCKSIZE * 2));
            expected.add(new Split(file4.getName(), BLOCKSIZE, 0));
            expected.add(new Split(file4.getName(), BLOCKSIZE, BLOCKSIZE));
            expected.add(new Split(file4.getName(), BLOCKSIZE, BLOCKSIZE * 2));
            actual.clear();
            for (int i = 0; i < 6; i++) {
              String name = fileSplit.getPath(i).getName();
              long length = fileSplit.getLength(i);
              long offset = fileSplit.getOffset(i);
              actual.add(new Split(name, length, offset));
            }
            assertTrue(actual.containsAll(expected));
            assertEquals(1, fileSplit.getLocations().length);
          }
        } else {
          fail("Split size should be 2 or 3.");
        }
      }

    assertEquals(3, nodeSplits.count(locations[1]));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testSplitPlacementForCompressedFiles() throws Exception {
    MiniDFSCluster dfs = null;
    FileSystem fileSys = null;
      for (InputSplit split : splits) {
        System.out.println("File split(Test1): " + split);
      }

      Set<Split> expected = new HashSet<>();
      expected.add(new Split(file1.getName(), f1.getLen(), 0));
      expected.add(new Split(file2.getName(), f2.getLen(), 0));
      List<Split> actual = new ArrayList<>();

      for (InputSplit split : splits) {
        fileSplit = (CombineFileSplit) split;
        if (splits.size() == 2) {
          if (split.equals(splits.get(0))) {
            assertEquals(1, fileSplit.getNumPaths());
            assertEquals(1, fileSplit.getLocations().length);
            assertEquals(file2.getName(), fileSplit.getPath(0).getName());
            assertEquals(0, fileSplit.getOffset(0));
            assertEquals(f2.getLen(), fileSplit.getLength(0));
            assertEquals(hosts2[0], fileSplit.getLocations()[0]);
          }
          if (split.equals(splits.get(1))) {
            assertEquals(1, fileSplit.getNumPaths());
            assertEquals(1, fileSplit.getLocations().length);
            assertEquals(file1.getName(), fileSplit.getPath(0).getName());
            assertEquals(0, fileSplit.getOffset(0));
            assertEquals(f1.getLen(), fileSplit.getLength(0));
            assertEquals(hosts1[0], fileSplit.getLocations()[0]);
          }
        } else if (splits.size() == 1) {
          assertEquals(2, fileSplit.getNumPaths());
          assertEquals(1, fileSplit.getLocations().length);
          assertEquals(hosts1[0], fileSplit.getLocations()[0]);
        } else {
          fail("Split size should be 1 or 2.");
        }
        for (int i = 0; i < fileSplit.getNumPaths(); i++) {
          String name = fileSplit.getPath(i).getName();
          long length = fileSplit.getLength(i);
          long offset = fileSplit.getOffset(i);
          actual.add(new Split(name, length, offset));
        }
      }
      assertEquals(2, actual.size());
      assertTrue(actual.containsAll(expected));

      dfs.startDataNodes(conf, 1, true, null, rack3, hosts3, null);
      for (InputSplit split : splits) {
        System.out.println("File split(Test2): " + split);
      }

      expected.add(new Split(file3.getName(), f3.getLen(), 0));
      actual.clear();

      for (InputSplit split : splits) {
        fileSplit = (CombineFileSplit) split;
        if (splits.size() == 3) {
          if (split.equals(splits.get(0))) {
            assertEquals(1, fileSplit.getNumPaths());
            assertEquals(file3.getName(), fileSplit.getPath(0).getName());
            assertEquals(f3.getLen(), fileSplit.getLength(0));
            assertEquals(0, fileSplit.getOffset(0));
            assertEquals(1, fileSplit.getLocations().length);
            assertEquals(hosts3[0], fileSplit.getLocations()[0]);
          }
          if (split.equals(splits.get(1))) {
            assertEquals(1, fileSplit.getNumPaths());
            assertEquals(file2.getName(), fileSplit.getPath(0).getName());
            assertEquals(f2.getLen(), fileSplit.getLength(0));
            assertEquals(0, fileSplit.getOffset(0));
            assertEquals(1, fileSplit.getLocations().length);
            assertEquals(hosts2[0], fileSplit.getLocations()[0]);
          }
          if (split.equals(splits.get(2))) {
            assertEquals(1, fileSplit.getNumPaths());
            assertEquals(file1.getName(), fileSplit.getPath(0).getName());
            assertEquals(f1.getLen(), fileSplit.getLength(0));
            assertEquals(0, fileSplit.getOffset(0));
            assertEquals(1, fileSplit.getLocations().length);
            assertEquals(hosts1[0], fileSplit.getLocations()[0]);
          }
        } else if (splits.size() == 2) {
          if (split.equals(splits.get(0))) {
            assertEquals(1, fileSplit.getLocations().length);
            if (fileSplit.getLocations()[0].equals(hosts2[0])) {
              assertEquals(2, fileSplit.getNumPaths());
            } else if (fileSplit.getLocations()[0].equals(hosts3[0])) {
              assertEquals(1, fileSplit.getNumPaths());
            } else {
              fail("First split should be on rack2 or rack3.");
            }
          }
          if (split.equals(splits.get(1))) {
            assertEquals(1, fileSplit.getLocations().length);
            assertEquals(hosts1[0], fileSplit.getLocations()[0]);
          }
        } else if (splits.size() == 1) {
          assertEquals(1, fileSplit.getLocations().length);
          assertEquals(3, fileSplit.getNumPaths());
          assertEquals(hosts1[0], fileSplit.getLocations()[0]);
        } else {
          fail("Split size should be 1, 2, or 3.");
        }
        for (int i = 0; i < fileSplit.getNumPaths(); i++) {
          String name = fileSplit.getPath(i).getName();
          long length = fileSplit.getLength(i);
          long offset = fileSplit.getOffset(i);
          actual.add(new Split(name, length, offset));
        }
      }

      assertEquals(3, actual.size());
      assertTrue(actual.containsAll(expected));

      Path file4 = new Path(dir4 + "/file4.gz");
      for (InputSplit split : splits) {
        System.out.println("File split(Test3): " + split);
      }

      expected.add(new Split(file3.getName(), f3.getLen(), 0));
      actual.clear();

      for (InputSplit split : splits) {
        fileSplit = (CombineFileSplit) split;
        if (splits.size() == 3) {
          if (split.equals(splits.get(0))) {
            assertEquals(2, fileSplit.getNumPaths());
            assertEquals(hosts3[0], fileSplit.getLocations()[0]);
          }
          if (split.equals(splits.get(1))) {
            assertEquals(1, fileSplit.getNumPaths());
            assertEquals(file2.getName(), fileSplit.getPath(0).getName());
            assertEquals(f2.getLen(), fileSplit.getLength(0));
            assertEquals(0, fileSplit.getOffset(0));
            assertEquals(1, fileSplit.getLocations().length);
            assertEquals(hosts2[0], fileSplit.getLocations()[0]);
          }
          if (split.equals(splits.get(2))) {
            assertEquals(1, fileSplit.getNumPaths());
            assertEquals(file1.getName(), fileSplit.getPath(0).getName());
            assertEquals(f1.getLen(), fileSplit.getLength(0));
            assertEquals(0, fileSplit.getOffset(0));
            assertEquals(1, fileSplit.getLocations().length);
            assertEquals(hosts1[0], fileSplit.getLocations()[0]);
          }
        } else if (splits.size() == 2) {
          if (split.equals(splits.get(0))) {
            assertEquals(1, fileSplit.getLocations().length);
            if (fileSplit.getLocations()[0].equals(hosts2[0])) {
              assertEquals(3, fileSplit.getNumPaths());
            } else if (fileSplit.getLocations()[0].equals(hosts3[0])) {
              assertEquals(2, fileSplit.getNumPaths());
            } else {
              fail("First split should be on rack2 or rack3.");
            }
          }
          if (split.equals(splits.get(1))) {
            assertEquals(1, fileSplit.getLocations().length);
            assertEquals(hosts1[0], fileSplit.getLocations()[0]);
          }
        } else if (splits.size() == 1) {
          assertEquals(1, fileSplit.getLocations().length);
          assertEquals(4, fileSplit.getNumPaths());
          assertEquals(hosts1[0], fileSplit.getLocations()[0]);
        } else {
          fail("Split size should be 1, 2, or 3.");
        }
        for (int i = 0; i < fileSplit.getNumPaths(); i++) {
          String name = fileSplit.getPath(i).getName();
          long length = fileSplit.getLength(i);
          long offset = fileSplit.getOffset(i);
          actual.add(new Split(name, length, offset));
        }
      }

      assertEquals(4, actual.size());
      assertTrue(actual.containsAll(expected));

      inFormat = new DummyInputFormat();
        System.out.println("File split(Test4): " + split);
      }
      assertEquals(4, splits.size());

      actual.clear();
      for (InputSplit split : splits) {
        fileSplit = (CombineFileSplit) split;
        for (int i = 0; i < fileSplit.getNumPaths(); i++) {
          String name = fileSplit.getPath(i).getName();
          long length = fileSplit.getLength(i);
          long offset = fileSplit.getOffset(i);
          actual.add(new Split(name, length, offset));
        }
        mockList.add(fileSplit.getLocations()[0]);
      }

      assertEquals(4, actual.size());
      assertTrue(actual.containsAll(expected));
      verify(mockList, atLeastOnce()).add(hosts1[0]);
      verify(mockList, atLeastOnce()).add(hosts2[0]);
      verify(mockList, atLeastOnce()).add(hosts3[0]);

      inFormat = new DummyInputFormat();
      for (InputSplit split : splits) {
        System.out.println("File split(Test5): " + split);
      }

      actual.clear();
      reset(mockList);
      for (InputSplit split : splits) {
        fileSplit = (CombineFileSplit) split;
        for (int i = 0; i < fileSplit.getNumPaths(); i++) {
          String name = fileSplit.getPath(i).getName();
          long length = fileSplit.getLength(i);
          long offset = fileSplit.getOffset(i);
          actual.add(new Split(name, length, offset));
        }
        mockList.add(fileSplit.getLocations()[0]);
      }
      assertEquals(4, actual.size());
      assertTrue(actual.containsAll(expected));

      if (splits.size() == 3) {
        verify(mockList, times(1)).add(hosts1[0]);
        verify(mockList, times(1)).add(hosts2[0]);
        verify(mockList, times(1)).add(hosts3[0]);
      } else if (splits.size() == 2) {
        verify(mockList, times(1)).add(hosts1[0]);
      } else {
        fail("Split size should be 2 or 3.");
      }

      inFormat = new DummyInputFormat();
      for (InputSplit split : splits) {
        System.out.println("File split(Test6): " + split);
      }

      assertTrue("Split size should be 1 or 2.",
          splits.size() == 1 || splits.size() == 2);
      actual.clear();
      reset(mockList);
      for (InputSplit split : splits) {
        fileSplit = (CombineFileSplit) split;
        for (int i = 0; i < fileSplit.getNumPaths(); i++) {
          String name = fileSplit.getPath(i).getName();
          long length = fileSplit.getLength(i);
          long offset = fileSplit.getOffset(i);
          actual.add(new Split(name, length, offset));
        }
        mockList.add(fileSplit.getLocations()[0]);
      }
      assertEquals(4, actual.size());
      assertTrue(actual.containsAll(expected));
      verify(mockList, times(1)).add(hosts1[0]);

      inFormat = new DummyInputFormat();
      for (InputSplit split : splits) {
        System.out.println("File split(Test9): " + split);
      }

      actual.clear();
      for (InputSplit split : splits) {
        fileSplit = (CombineFileSplit) split;
        if (splits.size() == 3) {
          if (split.equals(splits.get(0))) {
            assertEquals(1, fileSplit.getNumPaths());
            assertEquals(1, fileSplit.getLocations().length);
            assertEquals(hosts2[0], fileSplit.getLocations()[0]);
          }
          if (split.equals(splits.get(1))) {
            assertEquals(1, fileSplit.getNumPaths());
            assertEquals(1, fileSplit.getLocations().length);
            assertEquals(hosts1[0], fileSplit.getLocations()[0]);
          }
          if (split.equals(splits.get(2))) {
            assertEquals(2, fileSplit.getNumPaths());
            assertEquals(1, fileSplit.getLocations().length);
            assertEquals(hosts3[0], fileSplit.getLocations()[0]);
          }
        } else if (splits.size() == 2) {
          if (split.equals(splits.get(0))) {
            assertEquals(2, fileSplit.getNumPaths());
            assertEquals(1, fileSplit.getLocations().length);
            assertEquals(hosts1[0], fileSplit.getLocations()[0]);
          }
          if (split.equals(splits.get(1))) {
            assertEquals(2, fileSplit.getNumPaths());
            assertEquals(1, fileSplit.getLocations().length);
            assertEquals(hosts3[0], fileSplit.getLocations()[0]);
          }
        } else {
          fail("Split size should be 2 or 3.");
        }
        for (int i = 0; i < fileSplit.getNumPaths(); i++) {
          String name = fileSplit.getPath(i).getName();
          long length = fileSplit.getLength(i);
          long offset = fileSplit.getOffset(i);
          actual.add(new Split(name, length, offset));
        }
      }
      assertEquals(4, actual.size());
      assertTrue(actual.containsAll(expected));


