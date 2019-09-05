hadoop-tools/hadoop-aws/src/main/java/org/apache/hadoop/fs/s3a/Constants.java
  
  public static final String MIN_MULTIPART_THRESHOLD = "fs.s3a.multipart.threshold";
  public static final long DEFAULT_MIN_MULTIPART_THRESHOLD = Integer.MAX_VALUE;

  public static final String BUFFER_DIR = "fs.s3a.buffer.dir";
  public static final String SERVER_SIDE_ENCRYPTION_ALGORITHM = 
    "fs.s3a.server-side-encryption-algorithm";

  public static final String SIGNING_ALGORITHM = "fs.s3a.signing-algorithm";

  public static final String S3N_FOLDER_SUFFIX = "_$folder$";
  public static final String FS_S3A_BLOCK_SIZE = "fs.s3a.block.size";
  public static final String FS_S3A = "s3a";

hadoop-tools/hadoop-aws/src/main/java/org/apache/hadoop/fs/s3a/S3AFastOutputStream.java
  private ObjectMetadata createDefaultMetadata() {
    ObjectMetadata om = new ObjectMetadata();
    if (StringUtils.isNotBlank(serverSideEncryptionAlgorithm)) {
      om.setSSEAlgorithm(serverSideEncryptionAlgorithm);
    }
    return om;
  }

hadoop-tools/hadoop-aws/src/main/java/org/apache/hadoop/fs/s3a/S3AFileSystem.java
  private long partSize;
  private TransferManager transfers;
  private ThreadPoolExecutor threadPoolExecutor;
  private long multiPartThreshold;
  public static final Logger LOG = LoggerFactory.getLogger(S3AFileSystem.class);
  private CannedAccessControlList cannedACL;
  private String serverSideEncryptionAlgorithm;
        DEFAULT_ESTABLISH_TIMEOUT));
    awsConf.setSocketTimeout(conf.getInt(SOCKET_TIMEOUT, 
      DEFAULT_SOCKET_TIMEOUT));
    String signerOverride = conf.getTrimmed(SIGNING_ALGORITHM, "");
    if(!signerOverride.isEmpty()) {
      awsConf.setSignerOverride(signerOverride);
    }

    String proxyHost = conf.getTrimmed(PROXY_HOST, "");
    int proxyPort = conf.getInt(PROXY_PORT, -1);

    maxKeys = conf.getInt(MAX_PAGING_KEYS, DEFAULT_MAX_PAGING_KEYS);
    partSize = conf.getLong(MULTIPART_SIZE, DEFAULT_MULTIPART_SIZE);
    multiPartThreshold = conf.getLong(MIN_MULTIPART_THRESHOLD,
      DEFAULT_MIN_MULTIPART_THRESHOLD);

    if (partSize < 5 * 1024 * 1024) {
    if (getConf().getBoolean(FAST_UPLOAD, DEFAULT_FAST_UPLOAD)) {
      return new FSDataOutputStream(new S3AFastOutputStream(s3, this, bucket,
          key, progress, statistics, cannedACL,
          serverSideEncryptionAlgorithm, partSize, multiPartThreshold,
          threadPoolExecutor), statistics);
    }

    final ObjectMetadata om = new ObjectMetadata();
    if (StringUtils.isNotBlank(serverSideEncryptionAlgorithm)) {
      om.setSSEAlgorithm(serverSideEncryptionAlgorithm);
    }
    PutObjectRequest putObjectRequest = new PutObjectRequest(bucket, key, srcfile);
    putObjectRequest.setCannedAcl(cannedACL);

    ProgressListener progressListener = new ProgressListener() {
      public void progressChanged(ProgressEvent progressEvent) {
        switch (progressEvent.getEventType()) {
          case TRANSFER_PART_COMPLETED_EVENT:
            statistics.incrementWriteOps(1);
            break;
          default:
    ObjectMetadata srcom = s3.getObjectMetadata(bucket, srcKey);
    final ObjectMetadata dstom = srcom.clone();
    if (StringUtils.isNotBlank(serverSideEncryptionAlgorithm)) {
      dstom.setSSEAlgorithm(serverSideEncryptionAlgorithm);
    }
    CopyObjectRequest copyObjectRequest = new CopyObjectRequest(bucket, srcKey, bucket, dstKey);
    copyObjectRequest.setCannedAccessControlList(cannedACL);

    ProgressListener progressListener = new ProgressListener() {
      public void progressChanged(ProgressEvent progressEvent) {
        switch (progressEvent.getEventType()) {
          case TRANSFER_PART_COMPLETED_EVENT:
            statistics.incrementWriteOps(1);
            break;
          default:
    final ObjectMetadata om = new ObjectMetadata();
    om.setContentLength(0L);
    if (StringUtils.isNotBlank(serverSideEncryptionAlgorithm)) {
      om.setSSEAlgorithm(serverSideEncryptionAlgorithm);
    }
    PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, objectName, im, om);
    putObjectRequest.setCannedAcl(cannedACL);

hadoop-tools/hadoop-aws/src/main/java/org/apache/hadoop/fs/s3a/S3AOutputStream.java
package org.apache.hadoop.fs.s3a;

import com.amazonaws.event.ProgressEvent;
import com.amazonaws.event.ProgressEventType;
import com.amazonaws.event.ProgressListener;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import java.io.IOException;
import java.io.OutputStream;

import static com.amazonaws.event.ProgressEventType.TRANSFER_COMPLETED_EVENT;
import static com.amazonaws.event.ProgressEventType.TRANSFER_PART_STARTED_EVENT;
import static org.apache.hadoop.fs.s3a.Constants.*;

public class S3AOutputStream extends OutputStream {
  private TransferManager transfers;
  private Progressable progress;
  private long partSize;
  private long partSizeThreshold;
  private S3AFileSystem fs;
  private CannedAccessControlList cannedACL;
  private FileSystem.Statistics statistics;
    this.serverSideEncryptionAlgorithm = serverSideEncryptionAlgorithm;

    partSize = conf.getLong(MULTIPART_SIZE, DEFAULT_MULTIPART_SIZE);
    partSizeThreshold = conf.getLong(MIN_MULTIPART_THRESHOLD,
        DEFAULT_MIN_MULTIPART_THRESHOLD);

    if (conf.get(BUFFER_DIR, null) != null) {
      lDirAlloc = new LocalDirAllocator(BUFFER_DIR);
    try {
      final ObjectMetadata om = new ObjectMetadata();
      if (StringUtils.isNotBlank(serverSideEncryptionAlgorithm)) {
        om.setSSEAlgorithm(serverSideEncryptionAlgorithm);
      }
      PutObjectRequest putObjectRequest = new PutObjectRequest(bucket, key, backupFile);
      putObjectRequest.setCannedAcl(cannedACL);
      }

      ProgressEventType pet = progressEvent.getEventType();
      if (pet == TRANSFER_PART_STARTED_EVENT ||
          pet == TRANSFER_COMPLETED_EVENT) {
        statistics.incrementWriteOps(1);
      }


