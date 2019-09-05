hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/protocol/HdfsConstants.java
import org.apache.hadoop.util.StringUtils;

@InterfaceAudience.Private
public class HdfsConstants {
  public static final long QUOTA_DONT_SET = Long.MAX_VALUE;
  public static final long QUOTA_RESET = -1L;

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/protocol/FSConstants.java
@Deprecated
public final class FSConstants extends HdfsConstants {
}

