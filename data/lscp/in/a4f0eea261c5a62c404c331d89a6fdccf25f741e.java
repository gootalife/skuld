hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/BlockReader.java
import java.io.IOException;
import java.util.EnumSet;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.ByteBufferReadable;
import org.apache.hadoop.fs.ReadOption;
import org.apache.hadoop.hdfs.shortcircuit.ClientMmap;
@InterfaceAudience.Private
public interface BlockReader extends ByteBufferReadable {
  


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/BlockReaderLocal.java

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.ReadOption;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSClient.Conf;
@InterfaceAudience.Private
class BlockReaderLocal implements BlockReader {
  static final Log LOG = LogFactory.getLog(BlockReaderLocal.class);


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/BlockReaderLocalLegacy.java

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ReadOption;
import org.apache.hadoop.fs.StorageType;
@InterfaceAudience.Private
class BlockReaderLocalLegacy implements BlockReader {
  private static final Log LOG = LogFactory.getLog(BlockReaderLocalLegacy.class);


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/BlockReaderUtil.java
package org.apache.hadoop.hdfs;

import org.apache.hadoop.classification.InterfaceAudience;

import java.io.IOException;

@InterfaceAudience.Private
class BlockReaderUtil {


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/CorruptFileBlockIterator.java
hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/DFSHedgedReadMetrics.java
package org.apache.hadoop.hdfs;

import org.apache.hadoop.classification.InterfaceAudience;

import java.util.concurrent.atomic.AtomicLong;

@InterfaceAudience.Private
public class DFSHedgedReadMetrics {
  public final AtomicLong hedgedReadOps = new AtomicLong();
  public final AtomicLong hedgedReadOpsWin = new AtomicLong();

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/DFSPacket.java
import java.nio.channels.ClosedChannelException;
import java.util.Arrays;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.datatransfer.PacketHeader;
import org.apache.hadoop.hdfs.util.ByteArrayManager;

@InterfaceAudience.Private
class DFSPacket {
  public static final long HEART_BEAT_SEQNO = -1L;
  private static long[] EMPTY = new long[0];

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/DataStreamer.java
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.DSQuotaExceededException;

@InterfaceAudience.Private
class DataStreamer extends Daemon {

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/ExtendedBlockId.java

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;

@InterfaceAudience.Private
final public class ExtendedBlockId {

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/HAUtil.java
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

@InterfaceAudience.Private
public class HAUtil {
  
  private static final Log LOG = 

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/KeyProviderCache.java

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.key.KeyProvider;

import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

@InterfaceAudience.Private
public class KeyProviderCache {

  public static final Log LOG = LogFactory.getLog(KeyProviderCache.class);

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/LeaseRenewer.java
hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/NameNodeProxies.java

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSClient.Conf;
import org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException;
@InterfaceAudience.Private
public class NameNodeProxies {
  
  private static final Log LOG = LogFactory.getLog(NameNodeProxies.class);

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/RemotePeerFactory.java
import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.net.Peer;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.security.token.Token;

@InterfaceAudience.Private
public interface RemotePeerFactory {

