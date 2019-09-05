hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-shuffle/src/main/java/org/apache/hadoop/mapred/ShuffleHandler.java
public class ShuffleHandler extends AuxiliaryService {

  private static final Log LOG = LogFactory.getLog(ShuffleHandler.class);
  private static final Log AUDITLOG =
      LogFactory.getLog(ShuffleHandler.class.getName()+".audit");
  public static final String SHUFFLE_MANAGE_OS_CACHE = "mapreduce.shuffle.manage.os.cache";
  public static final boolean DEFAULT_SHUFFLE_MANAGE_OS_CACHE = true;

        sendError(ctx, "Too many job/reduce parameters", BAD_REQUEST);
        return;
      }

      if (AUDITLOG.isDebugEnabled()) {
        AUDITLOG.debug("shuffle for " + jobQ.get(0) +
                         " reducer " + reduceQ.get(0));
      }
      int reduceId;
      String jobId;
      try {
    protected void setResponseHeaders(HttpResponse response,
        boolean keepAliveParam, long contentLength) {
      if (!connectionKeepAliveEnabled && !keepAliveParam) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Setting connection close header...");
        }
        response.setHeader(HttpHeaders.CONNECTION, CONNECTION_CLOSE);
      } else {
        response.setHeader(HttpHeaders.CONTENT_LENGTH,

