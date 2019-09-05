hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/InvalidateBlocks.java
import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.DFSConfigKeys;
class InvalidateBlocks {
  private final Map<DatanodeInfo, LightWeightHashSet<Block>> node2blocks =
      new HashMap<DatanodeInfo, LightWeightHashSet<Block>>();
  private long numBlocks = 0L;


