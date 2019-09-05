hadoop-yarn-project/hadoop-yarn/hadoop-yarn-api/src/main/java/org/apache/hadoop/yarn/api/records/timeline/TimelinePutResponse.java
    public static final int FORBIDDEN_RELATION = 6;

    public static final int EXPIRED_ENTITY = 7;

    private String entityId;
    private String entityType;
    private int errorCode;

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-api/src/main/java/org/apache/hadoop/yarn/conf/YarnConfiguration.java
  public static final long DEFAULT_TIMELINE_SERVICE_TTL_MS =
      1000 * 60 * 60 * 24 * 7;

  public static final String TIMELINE_SERVICE_ROLLING_PERIOD =
      TIMELINE_SERVICE_PREFIX + "rolling-period";

  public static final String DEFAULT_TIMELINE_SERVICE_ROLLING_PERIOD =
      "hourly";

  public static final String TIMELINE_SERVICE_LEVELDB_PREFIX =
      TIMELINE_SERVICE_PREFIX + "leveldb-timeline-store.";

  public static final String TIMELINE_SERVICE_LEVELDB_PATH =
      TIMELINE_SERVICE_LEVELDB_PREFIX + "path";

  public static final String TIMELINE_SERVICE_LEVELDB_READ_CACHE_SIZE =
      TIMELINE_SERVICE_LEVELDB_PREFIX + "read-cache-size";

  public static final long DEFAULT_TIMELINE_SERVICE_LEVELDB_READ_CACHE_SIZE =
      100 * 1024 * 1024;

  public static final String TIMELINE_SERVICE_LEVELDB_WRITE_BUFFER_SIZE =
      TIMELINE_SERVICE_LEVELDB_PREFIX + "write-buffer-size";

  public static final int DEFAULT_TIMELINE_SERVICE_LEVELDB_WRITE_BUFFER_SIZE =
      16 * 1024 * 1024;

  public static final String
      TIMELINE_SERVICE_LEVELDB_WRITE_BATCH_SIZE =
      TIMELINE_SERVICE_LEVELDB_PREFIX + "write-batch-size";

  public static final int
      DEFAULT_TIMELINE_SERVICE_LEVELDB_WRITE_BATCH_SIZE = 10000;

  public static final String
      TIMELINE_SERVICE_LEVELDB_START_TIME_READ_CACHE_SIZE =
  public static final long DEFAULT_TIMELINE_SERVICE_LEVELDB_TTL_INTERVAL_MS =
      1000 * 60 * 5;

  public static final String TIMELINE_SERVICE_LEVELDB_MAX_OPEN_FILES =
      TIMELINE_SERVICE_LEVELDB_PREFIX + "max-open-files";

  public static final int DEFAULT_TIMELINE_SERVICE_LEVELDB_MAX_OPEN_FILES =
      1000;

  public static final String TIMELINE_SERVICE_PRINCIPAL =
      TIMELINE_SERVICE_PREFIX + "principal";

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-applicationhistoryservice/src/main/java/org/apache/hadoop/yarn/server/timeline/RollingLevelDB.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-applicationhistoryservice/src/main/java/org/apache/hadoop/yarn/server/timeline/RollingLevelDB.java

package org.apache.hadoop.yarn.server.timeline;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.time.FastDateFormat;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.fusesource.leveldbjni.JniDBFactory;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.WriteBatch;

class RollingLevelDB {

  private static final Log LOG = LogFactory.getLog(RollingLevelDB.class);
  private static JniDBFactory factory = new JniDBFactory();
  private FastDateFormat fdf;
  private SimpleDateFormat sdf;
  private GregorianCalendar cal = new GregorianCalendar(
      TimeZone.getTimeZone("GMT"));
  private final TreeMap<Long, DB> rollingdbs;
  private final TreeMap<Long, DB> rollingdbsToEvict;
  private final String name;
  private volatile long nextRollingCheckMillis = 0;
  private FileSystem lfs = null;
  private Path rollingDBPath;
  private Configuration conf;
  private RollingPeriod rollingPeriod;
  private long ttl;
  private boolean ttlEnabled;

  enum RollingPeriod {
    DAILY {
      @Override
      public String dateFormat() {
        return "yyyy-MM-dd";
      }
    },
    HALF_DAILY {
      @Override
      public String dateFormat() {
        return "yyyy-MM-dd-HH";
      }
    },
    QUARTER_DAILY {
      @Override
      public String dateFormat() {
        return "yyyy-MM-dd-HH";
      }
    },
    HOURLY {
      @Override
      public String dateFormat() {
        return "yyyy-MM-dd-HH";
      }
    },
    MINUTELY {
      @Override
      public String dateFormat() {
        return "yyyy-MM-dd-HH-mm";
      }
    };
    public abstract String dateFormat();
  }

  public static class RollingWriteBatch {
    private final DB db;
    private final WriteBatch writeBatch;

    public RollingWriteBatch(final DB db, final WriteBatch writeBatch) {
      this.db = db;
      this.writeBatch = writeBatch;
    }

    public DB getDB() {
      return db;
    }

    public WriteBatch getWriteBatch() {
      return writeBatch;
    }

    public void write() {
      db.write(writeBatch);
    }

    public void close() {
      IOUtils.cleanup(LOG, writeBatch);
    }
  }

  RollingLevelDB(String name) {
    this.name = name;
    this.rollingdbs = new TreeMap<Long, DB>();
    this.rollingdbsToEvict = new TreeMap<Long, DB>();
  }

  protected String getName() {
    return name;
  }

  protected long currentTimeMillis() {
    return System.currentTimeMillis();
  }

  public long getNextRollingTimeMillis() {
    return nextRollingCheckMillis;
  }

  public long getTimeToLive() {
    return ttl;
  }

  public boolean getTimeToLiveEnabled() {
    return ttlEnabled;
  }

  protected void setNextRollingTimeMillis(final long timestamp) {
    this.nextRollingCheckMillis = timestamp;
    LOG.info("Next rolling time for " + getName() + " is "
        + fdf.format(nextRollingCheckMillis));
  }

  public void init(final Configuration config) throws Exception {
    LOG.info("Initializing RollingLevelDB for " + getName());
    this.conf = config;
    this.ttl = conf.getLong(YarnConfiguration.TIMELINE_SERVICE_TTL_MS,
        YarnConfiguration.DEFAULT_TIMELINE_SERVICE_TTL_MS);
    this.ttlEnabled = conf.getBoolean(
        YarnConfiguration.TIMELINE_SERVICE_TTL_ENABLE, true);
    this.rollingDBPath = new Path(
        conf.get(YarnConfiguration.TIMELINE_SERVICE_LEVELDB_PATH),
        RollingLevelDBTimelineStore.FILENAME);
    initFileSystem();
    initRollingPeriod();
    initHistoricalDBs();
  }

  protected void initFileSystem() throws IOException {
    lfs = FileSystem.getLocal(conf);
    boolean success = lfs.mkdirs(rollingDBPath,
        RollingLevelDBTimelineStore.LEVELDB_DIR_UMASK);
    if (!success) {
      throw new IOException("Failed to create leveldb root directory "
          + rollingDBPath);
    }
  }

  protected synchronized void initRollingPeriod() {
    final String lcRollingPeriod = conf.get(
        YarnConfiguration.TIMELINE_SERVICE_ROLLING_PERIOD,
        YarnConfiguration.DEFAULT_TIMELINE_SERVICE_ROLLING_PERIOD);
    this.rollingPeriod = RollingPeriod.valueOf(lcRollingPeriod
        .toUpperCase(Locale.ENGLISH));
    fdf = FastDateFormat.getInstance(rollingPeriod.dateFormat(),
        TimeZone.getTimeZone("GMT"));
    sdf = new SimpleDateFormat(rollingPeriod.dateFormat());
    sdf.setTimeZone(fdf.getTimeZone());
  }

  protected synchronized void initHistoricalDBs() throws IOException {
    Path rollingDBGlobPath = new Path(rollingDBPath, getName() + ".*");
    FileStatus[] statuses = lfs.globStatus(rollingDBGlobPath);
    for (FileStatus status : statuses) {
      String dbName = FilenameUtils.getExtension(status.getPath().toString());
      try {
        Long dbStartTime = sdf.parse(dbName).getTime();
        initRollingLevelDB(dbStartTime, status.getPath());
      } catch (ParseException pe) {
        LOG.warn("Failed to initialize rolling leveldb " + dbName + " for "
            + getName());
      }
    }
  }

  private void initRollingLevelDB(Long dbStartTime,
      Path rollingInstanceDBPath) {
    if (rollingdbs.containsKey(dbStartTime)) {
      return;
    }
    Options options = new Options();
    options.createIfMissing(true);
    options.cacheSize(conf.getLong(
        YarnConfiguration.TIMELINE_SERVICE_LEVELDB_READ_CACHE_SIZE,
        YarnConfiguration.DEFAULT_TIMELINE_SERVICE_LEVELDB_READ_CACHE_SIZE));
    options.maxOpenFiles(conf.getInt(
        YarnConfiguration.TIMELINE_SERVICE_LEVELDB_MAX_OPEN_FILES,
        YarnConfiguration.DEFAULT_TIMELINE_SERVICE_LEVELDB_MAX_OPEN_FILES));
    options.writeBufferSize(conf.getInt(
        YarnConfiguration.TIMELINE_SERVICE_LEVELDB_WRITE_BUFFER_SIZE,
        YarnConfiguration.DEFAULT_TIMELINE_SERVICE_LEVELDB_WRITE_BUFFER_SIZE));
    LOG.info("Initializing rolling leveldb instance :" + rollingInstanceDBPath
        + " for start time: " + dbStartTime);
    DB db = null;
    try {
      db = factory.open(
          new File(rollingInstanceDBPath.toUri().getPath()), options);
      rollingdbs.put(dbStartTime, db);
      String dbName = fdf.format(dbStartTime);
      LOG.info("Added rolling leveldb instance " + dbName + " to " + getName());
    } catch (IOException ioe) {
      LOG.warn("Failed to open rolling leveldb instance :"
          + new File(rollingInstanceDBPath.toUri().getPath()), ioe);
    }
  }

  synchronized DB getPreviousDB(DB db) {
    Iterator<DB> iterator = rollingdbs.values().iterator();
    DB prev = null;
    while (iterator.hasNext()) {
      DB cur = iterator.next();
      if (cur == db) {
        break;
      }
      prev = cur;
    }
    return prev;
  }

  synchronized long getStartTimeFor(DB db) {
    long startTime = -1;
    for (Map.Entry<Long, DB> entry : rollingdbs.entrySet()) {
      if (entry.getValue() == db) {
        startTime = entry.getKey();
      }
    }
    return startTime;
  }

  public synchronized DB getDBForStartTime(long startTime) {
    startTime = Math.min(startTime, currentTimeMillis());

    if (startTime >= getNextRollingTimeMillis()) {
      roll(startTime);
    }
    Entry<Long, DB> entry = rollingdbs.floorEntry(startTime);
    if (entry == null) {
      return null;
    }
    return entry.getValue();
  }

  private void roll(long startTime) {
    LOG.info("Rolling new DB instance for " + getName());
    long currentStartTime = computeCurrentCheckMillis(startTime);
    setNextRollingTimeMillis(computeNextCheckMillis(currentStartTime));
    String currentRollingDBInstance = fdf.format(currentStartTime);
    String currentRollingDBName = getName() + "." + currentRollingDBInstance;
    Path currentRollingDBPath = new Path(rollingDBPath, currentRollingDBName);
    if (getTimeToLiveEnabled()) {
      scheduleOldDBsForEviction();
    }
    initRollingLevelDB(currentStartTime, currentRollingDBPath);
  }

  private synchronized void scheduleOldDBsForEviction() {
    long evictionThreshold = computeCurrentCheckMillis(currentTimeMillis()
        - getTimeToLive());

    LOG.info("Scheduling " + getName() + " DBs older than "
        + fdf.format(evictionThreshold) + " for eviction");
    Iterator<Entry<Long, DB>> iterator = rollingdbs.entrySet().iterator();
    while (iterator.hasNext()) {
      Entry<Long, DB> entry = iterator.next();
      if (entry.getKey() < evictionThreshold) {
        LOG.info("Scheduling " + getName() + " eviction for "
            + fdf.format(entry.getKey()));
        iterator.remove();
        rollingdbsToEvict.put(entry.getKey(), entry.getValue());
      }
    }
  }

  public synchronized void evictOldDBs() {
    LOG.info("Evicting " + getName() + " DBs scheduled for eviction");
    Iterator<Entry<Long, DB>> iterator = rollingdbsToEvict.entrySet()
        .iterator();
    while (iterator.hasNext()) {
      Entry<Long, DB> entry = iterator.next();
      IOUtils.cleanup(LOG, entry.getValue());
      String dbName = fdf.format(entry.getKey());
      Path path = new Path(rollingDBPath, getName() + "." + dbName);
      try {
        LOG.info("Removing old db directory contents in " + path);
        lfs.delete(path, true);
      } catch (IOException ioe) {
        LOG.warn("Failed to evict old db " + path, ioe);
      }
      iterator.remove();
    }
  }

  public void stop() throws Exception {
    for (DB db : rollingdbs.values()) {
      IOUtils.cleanup(LOG, db);
    }
    IOUtils.cleanup(LOG, lfs);
  }

  private long computeNextCheckMillis(long now) {
    return computeCheckMillis(now, true);
  }

  public long computeCurrentCheckMillis(long now) {
    return computeCheckMillis(now, false);
  }

  private synchronized long computeCheckMillis(long now, boolean next) {
    cal.setTimeInMillis(now);
    cal.set(Calendar.SECOND, 0);
    cal.set(Calendar.MILLISECOND, 0);

    if (rollingPeriod == RollingPeriod.DAILY) {
      cal.set(Calendar.HOUR_OF_DAY, 0);
      cal.set(Calendar.MINUTE, 0);
      if (next) {
        cal.add(Calendar.DATE, 1);
      }
    } else if (rollingPeriod == RollingPeriod.HALF_DAILY) {
      int hour = (cal.get(Calendar.HOUR) / 12) * 12;
      cal.set(Calendar.HOUR, hour);
      cal.set(Calendar.MINUTE, 0);
      if (next) {
        cal.add(Calendar.HOUR_OF_DAY, 12);
      }
    } else if (rollingPeriod == RollingPeriod.QUARTER_DAILY) {
      int hour = (cal.get(Calendar.HOUR) / 6) * 6;
      cal.set(Calendar.HOUR, hour);
      cal.set(Calendar.MINUTE, 0);
      if (next) {
        cal.add(Calendar.HOUR_OF_DAY, 6);
      }
    } else if (rollingPeriod == RollingPeriod.HOURLY) {
      cal.set(Calendar.MINUTE, 0);
      if (next) {
        cal.add(Calendar.HOUR_OF_DAY, 1);
      }
    } else if (rollingPeriod == RollingPeriod.MINUTELY) {
      int minute = (cal.get(Calendar.MINUTE) / 5) * 5;
      cal.set(Calendar.MINUTE, minute);
      if (next) {
        cal.add(Calendar.MINUTE, 5);
      }
    }
    return cal.getTimeInMillis();
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-applicationhistoryservice/src/main/java/org/apache/hadoop/yarn/server/timeline/RollingLevelDBTimelineStore.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-applicationhistoryservice/src/main/java/org/apache/hadoop/yarn/server/timeline/RollingLevelDBTimelineStore.java

package org.apache.hadoop.yarn.server.timeline;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;

import org.apache.commons.collections.map.LRUMap;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.timeline.TimelineDomain;
import org.apache.hadoop.yarn.api.records.timeline.TimelineDomains;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEvent;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEvents;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEvents.EventsOfOneEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelinePutResponse;
import org.apache.hadoop.yarn.api.records.timeline.TimelinePutResponse.TimelinePutError;
import org.apache.hadoop.yarn.proto.YarnServerCommonProtos.VersionProto;
import org.apache.hadoop.yarn.server.records.Version;
import org.apache.hadoop.yarn.server.records.impl.pb.VersionPBImpl;
import org.apache.hadoop.yarn.server.timeline.RollingLevelDB.RollingWriteBatch;
import org.apache.hadoop.yarn.server.timeline.TimelineDataManager.CheckAcl;
import org.apache.hadoop.yarn.server.timeline.util.LeveldbUtils.KeyBuilder;
import org.apache.hadoop.yarn.server.timeline.util.LeveldbUtils.KeyParser;

import org.fusesource.leveldbjni.JniDBFactory;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBException;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.ReadOptions;
import org.iq80.leveldb.WriteBatch;
import org.nustaq.serialization.FSTConfiguration;

import static java.nio.charset.StandardCharsets.UTF_8;

import static org.apache.hadoop.yarn.server.timeline.GenericObjectMapper.readReverseOrderedLong;
import static org.apache.hadoop.yarn.server.timeline.GenericObjectMapper.writeReverseOrderedLong;
import static org.apache.hadoop.yarn.server.timeline.TimelineDataManager.DEFAULT_DOMAIN_ID;
import static org.apache.hadoop.yarn.server.timeline.util.LeveldbUtils.prefixMatches;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.DEFAULT_TIMELINE_SERVICE_LEVELDB_MAX_OPEN_FILES;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.DEFAULT_TIMELINE_SERVICE_LEVELDB_READ_CACHE_SIZE;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.DEFAULT_TIMELINE_SERVICE_LEVELDB_START_TIME_READ_CACHE_SIZE;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.DEFAULT_TIMELINE_SERVICE_LEVELDB_START_TIME_WRITE_CACHE_SIZE;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.DEFAULT_TIMELINE_SERVICE_LEVELDB_TTL_INTERVAL_MS;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.DEFAULT_TIMELINE_SERVICE_LEVELDB_WRITE_BATCH_SIZE;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.DEFAULT_TIMELINE_SERVICE_LEVELDB_WRITE_BUFFER_SIZE;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.DEFAULT_TIMELINE_SERVICE_TTL_MS;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.TIMELINE_SERVICE_LEVELDB_MAX_OPEN_FILES;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.TIMELINE_SERVICE_LEVELDB_PATH;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.TIMELINE_SERVICE_LEVELDB_READ_CACHE_SIZE;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.TIMELINE_SERVICE_LEVELDB_START_TIME_READ_CACHE_SIZE;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.TIMELINE_SERVICE_LEVELDB_START_TIME_WRITE_CACHE_SIZE;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.TIMELINE_SERVICE_LEVELDB_TTL_INTERVAL_MS;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.TIMELINE_SERVICE_LEVELDB_WRITE_BATCH_SIZE;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.TIMELINE_SERVICE_LEVELDB_WRITE_BUFFER_SIZE;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.TIMELINE_SERVICE_TTL_ENABLE;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.TIMELINE_SERVICE_TTL_MS;

import static org.fusesource.leveldbjni.JniDBFactory.bytes;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class RollingLevelDBTimelineStore extends AbstractService implements
    TimelineStore {
  private static final Log LOG = LogFactory
      .getLog(RollingLevelDBTimelineStore.class);
  private static FSTConfiguration fstConf =
      FSTConfiguration.createDefaultConfiguration();

  static {
    fstConf.setShareReferences(false);
  }

  @Private
  @VisibleForTesting
  static final String FILENAME = "leveldb-timeline-store";
  static final String DOMAIN = "domain-ldb";
  static final String ENTITY = "entity-ldb";
  static final String INDEX = "indexes-ldb";
  static final String STARTTIME = "starttime-ldb";
  static final String OWNER = "owner-ldb";

  private static final byte[] DOMAIN_ID_COLUMN = "d".getBytes(UTF_8);
  private static final byte[] EVENTS_COLUMN = "e".getBytes(UTF_8);
  private static final byte[] PRIMARY_FILTERS_COLUMN = "f".getBytes(UTF_8);
  private static final byte[] OTHER_INFO_COLUMN = "i".getBytes(UTF_8);
  private static final byte[] RELATED_ENTITIES_COLUMN = "r".getBytes(UTF_8);

  private static final byte[] DESCRIPTION_COLUMN = "d".getBytes(UTF_8);
  private static final byte[] OWNER_COLUMN = "o".getBytes(UTF_8);
  private static final byte[] READER_COLUMN = "r".getBytes(UTF_8);
  private static final byte[] WRITER_COLUMN = "w".getBytes(UTF_8);
  private static final byte[] TIMESTAMP_COLUMN = "t".getBytes(UTF_8);

  private static final byte[] EMPTY_BYTES = new byte[0];

  private static final String TIMELINE_STORE_VERSION_KEY =
      "timeline-store-version";

  private static final Version CURRENT_VERSION_INFO = Version.newInstance(1, 0);

  private static long writeBatchSize = 10000;

  @Private
  @VisibleForTesting
  static final FsPermission LEVELDB_DIR_UMASK = FsPermission
      .createImmutable((short) 0700);

  private Map<EntityIdentifier, Long> startTimeWriteCache;
  private Map<EntityIdentifier, Long> startTimeReadCache;

  private DB domaindb;
  private RollingLevelDB entitydb;
  private RollingLevelDB indexdb;
  private DB starttimedb;
  private DB ownerdb;

  private Thread deletionThread;

  public RollingLevelDBTimelineStore() {
    super(RollingLevelDBTimelineStore.class.getName());
  }

  @Override
  @SuppressWarnings("unchecked")
  protected void serviceInit(Configuration conf) throws Exception {
    Preconditions
        .checkArgument(conf.getLong(TIMELINE_SERVICE_TTL_MS,
            DEFAULT_TIMELINE_SERVICE_TTL_MS) > 0,
            "%s property value should be greater than zero",
            TIMELINE_SERVICE_TTL_MS);
    Preconditions.checkArgument(conf.getLong(
        TIMELINE_SERVICE_LEVELDB_TTL_INTERVAL_MS,
        DEFAULT_TIMELINE_SERVICE_LEVELDB_TTL_INTERVAL_MS) > 0,
        "%s property value should be greater than zero",
        TIMELINE_SERVICE_LEVELDB_TTL_INTERVAL_MS);
    Preconditions.checkArgument(conf.getLong(
        TIMELINE_SERVICE_LEVELDB_READ_CACHE_SIZE,
        DEFAULT_TIMELINE_SERVICE_LEVELDB_READ_CACHE_SIZE) >= 0,
        "%s property value should be greater than or equal to zero",
        TIMELINE_SERVICE_LEVELDB_READ_CACHE_SIZE);
    Preconditions.checkArgument(conf.getLong(
        TIMELINE_SERVICE_LEVELDB_START_TIME_READ_CACHE_SIZE,
        DEFAULT_TIMELINE_SERVICE_LEVELDB_START_TIME_READ_CACHE_SIZE) > 0,
        " %s property value should be greater than zero",
        TIMELINE_SERVICE_LEVELDB_START_TIME_READ_CACHE_SIZE);
    Preconditions.checkArgument(conf.getLong(
        TIMELINE_SERVICE_LEVELDB_START_TIME_WRITE_CACHE_SIZE,
        DEFAULT_TIMELINE_SERVICE_LEVELDB_START_TIME_WRITE_CACHE_SIZE) > 0,
        "%s property value should be greater than zero",
        TIMELINE_SERVICE_LEVELDB_START_TIME_WRITE_CACHE_SIZE);
    Preconditions.checkArgument(conf.getLong(
        TIMELINE_SERVICE_LEVELDB_MAX_OPEN_FILES,
        DEFAULT_TIMELINE_SERVICE_LEVELDB_MAX_OPEN_FILES) > 0,
        "%s property value should be greater than zero",
        TIMELINE_SERVICE_LEVELDB_MAX_OPEN_FILES);
    Preconditions.checkArgument(conf.getLong(
        TIMELINE_SERVICE_LEVELDB_WRITE_BUFFER_SIZE,
        DEFAULT_TIMELINE_SERVICE_LEVELDB_WRITE_BUFFER_SIZE) > 0,
        "%s property value should be greater than zero",
        TIMELINE_SERVICE_LEVELDB_WRITE_BUFFER_SIZE);

    Options options = new Options();
    options.createIfMissing(true);
    options.cacheSize(conf.getLong(
        TIMELINE_SERVICE_LEVELDB_READ_CACHE_SIZE,
        DEFAULT_TIMELINE_SERVICE_LEVELDB_READ_CACHE_SIZE));
    JniDBFactory factory = new JniDBFactory();
    Path dbPath = new Path(
        conf.get(TIMELINE_SERVICE_LEVELDB_PATH), FILENAME);
    Path domainDBPath = new Path(dbPath, DOMAIN);
    Path starttimeDBPath = new Path(dbPath, STARTTIME);
    Path ownerDBPath = new Path(dbPath, OWNER);
    FileSystem localFS = null;
    try {
      localFS = FileSystem.getLocal(conf);
      if (!localFS.exists(dbPath)) {
        if (!localFS.mkdirs(dbPath)) {
          throw new IOException("Couldn't create directory for leveldb "
              + "timeline store " + dbPath);
        }
        localFS.setPermission(dbPath, LEVELDB_DIR_UMASK);
      }
      if (!localFS.exists(domainDBPath)) {
        if (!localFS.mkdirs(domainDBPath)) {
          throw new IOException("Couldn't create directory for leveldb "
              + "timeline store " + domainDBPath);
        }
        localFS.setPermission(domainDBPath, LEVELDB_DIR_UMASK);
      }
      if (!localFS.exists(starttimeDBPath)) {
        if (!localFS.mkdirs(starttimeDBPath)) {
          throw new IOException("Couldn't create directory for leveldb "
              + "timeline store " + starttimeDBPath);
        }
        localFS.setPermission(starttimeDBPath, LEVELDB_DIR_UMASK);
      }
      if (!localFS.exists(ownerDBPath)) {
        if (!localFS.mkdirs(ownerDBPath)) {
          throw new IOException("Couldn't create directory for leveldb "
              + "timeline store " + ownerDBPath);
        }
        localFS.setPermission(ownerDBPath, LEVELDB_DIR_UMASK);
      }
    } finally {
      IOUtils.cleanup(LOG, localFS);
    }
    options.maxOpenFiles(conf.getInt(
        TIMELINE_SERVICE_LEVELDB_MAX_OPEN_FILES,
        DEFAULT_TIMELINE_SERVICE_LEVELDB_MAX_OPEN_FILES));
    options.writeBufferSize(conf.getInt(
        TIMELINE_SERVICE_LEVELDB_WRITE_BUFFER_SIZE,
        DEFAULT_TIMELINE_SERVICE_LEVELDB_WRITE_BUFFER_SIZE));
    LOG.info("Using leveldb path " + dbPath);
    domaindb = factory.open(new File(domainDBPath.toString()), options);
    entitydb = new RollingLevelDB(ENTITY);
    entitydb.init(conf);
    indexdb = new RollingLevelDB(INDEX);
    indexdb.init(conf);
    starttimedb = factory.open(new File(starttimeDBPath.toString()), options);
    ownerdb = factory.open(new File(ownerDBPath.toString()), options);
    checkVersion();
    startTimeWriteCache = Collections.synchronizedMap(new LRUMap(
        getStartTimeWriteCacheSize(conf)));
    startTimeReadCache = Collections.synchronizedMap(new LRUMap(
        getStartTimeReadCacheSize(conf)));

    writeBatchSize = conf.getInt(
        TIMELINE_SERVICE_LEVELDB_WRITE_BATCH_SIZE,
        DEFAULT_TIMELINE_SERVICE_LEVELDB_WRITE_BATCH_SIZE);

    super.serviceInit(conf);
  }
  
  @Override
  protected void serviceStart() throws Exception {
    if (getConfig().getBoolean(TIMELINE_SERVICE_TTL_ENABLE, true)) {
      deletionThread = new EntityDeletionThread(getConfig());
      deletionThread.start();
    }
    super.serviceStart();
   }

  @Override
  protected void serviceStop() throws Exception {
    if (deletionThread != null) {
      deletionThread.interrupt();
      LOG.info("Waiting for deletion thread to complete its current action");
      try {
        deletionThread.join();
      } catch (InterruptedException e) {
        LOG.warn("Interrupted while waiting for deletion thread to complete,"
            + " closing db now", e);
      }
    }
    IOUtils.cleanup(LOG, domaindb);
    IOUtils.cleanup(LOG, starttimedb);
    IOUtils.cleanup(LOG, ownerdb);
    entitydb.stop();
    indexdb.stop();
    super.serviceStop();
  }

  private class EntityDeletionThread extends Thread {
    private final long ttl;
    private final long ttlInterval;

    public EntityDeletionThread(Configuration conf) {
      ttl = conf.getLong(TIMELINE_SERVICE_TTL_MS,
          DEFAULT_TIMELINE_SERVICE_TTL_MS);
      ttlInterval = conf.getLong(
          TIMELINE_SERVICE_LEVELDB_TTL_INTERVAL_MS,
          DEFAULT_TIMELINE_SERVICE_LEVELDB_TTL_INTERVAL_MS);
      LOG.info("Starting deletion thread with ttl " + ttl + " and cycle "
          + "interval " + ttlInterval);
    }

    @Override
    public void run() {
      Thread.currentThread().setName("Leveldb Timeline Store Retention");
      while (true) {
        long timestamp = System.currentTimeMillis() - ttl;
        try {
          discardOldEntities(timestamp);
          Thread.sleep(ttlInterval);
        } catch (IOException e) {
          LOG.error(e);
        } catch (InterruptedException e) {
          LOG.info("Deletion thread received interrupt, exiting");
          break;
        }
      }
    }
  }

  @Override
  public TimelineEntity getEntity(String entityId, String entityType,
      EnumSet<Field> fields) throws IOException {
    Long revStartTime = getStartTimeLong(entityId, entityType);
    if (revStartTime == null) {
      return null;
    }
    byte[] prefix = KeyBuilder.newInstance().add(entityType)
        .add(writeReverseOrderedLong(revStartTime)).add(entityId)
        .getBytesForLookup();

    DBIterator iterator = null;
    try {
      DB db = entitydb.getDBForStartTime(revStartTime);
      if (db == null) {
        return null;
      }
      iterator = db.iterator();
      iterator.seek(prefix);

      return getEntity(entityId, entityType, revStartTime, fields, iterator,
          prefix, prefix.length);
    } finally {
      IOUtils.cleanup(LOG, iterator);
    }
  }

  private static TimelineEntity getEntity(String entityId, String entityType,
      Long startTime, EnumSet<Field> fields, DBIterator iterator,
      byte[] prefix, int prefixlen) throws IOException {
    if (fields == null) {
      fields = EnumSet.allOf(Field.class);
    }

    TimelineEntity entity = new TimelineEntity();
    boolean events = false;
    boolean lastEvent = false;
    if (fields.contains(Field.EVENTS)) {
      events = true;
    } else if (fields.contains(Field.LAST_EVENT_ONLY)) {
      lastEvent = true;
    } else {
      entity.setEvents(null);
    }
    boolean relatedEntities = false;
    if (fields.contains(Field.RELATED_ENTITIES)) {
      relatedEntities = true;
    } else {
      entity.setRelatedEntities(null);
    }
    boolean primaryFilters = false;
    if (fields.contains(Field.PRIMARY_FILTERS)) {
      primaryFilters = true;
    } else {
      entity.setPrimaryFilters(null);
    }
    boolean otherInfo = false;
    if (fields.contains(Field.OTHER_INFO)) {
      otherInfo = true;
    } else {
      entity.setOtherInfo(null);
    }

    for (; iterator.hasNext(); iterator.next()) {
      byte[] key = iterator.peekNext().getKey();
      if (!prefixMatches(prefix, prefixlen, key)) {
        break;
      }
      if (key.length == prefixlen) {
        continue;
      }
      if (key[prefixlen] == PRIMARY_FILTERS_COLUMN[0]) {
        if (primaryFilters) {
          addPrimaryFilter(entity, key, prefixlen
              + PRIMARY_FILTERS_COLUMN.length);
        }
      } else if (key[prefixlen] == OTHER_INFO_COLUMN[0]) {
        if (otherInfo) {
          entity.addOtherInfo(
              parseRemainingKey(key, prefixlen + OTHER_INFO_COLUMN.length),
              fstConf.asObject(iterator.peekNext().getValue()));
        }
      } else if (key[prefixlen] == RELATED_ENTITIES_COLUMN[0]) {
        if (relatedEntities) {
          addRelatedEntity(entity, key, prefixlen
              + RELATED_ENTITIES_COLUMN.length);
        }
      } else if (key[prefixlen] == EVENTS_COLUMN[0]) {
        if (events || (lastEvent && entity.getEvents().size() == 0)) {
          TimelineEvent event = getEntityEvent(null, key, prefixlen
              + EVENTS_COLUMN.length, iterator.peekNext().getValue());
          if (event != null) {
            entity.addEvent(event);
          }
        }
      } else if (key[prefixlen] == DOMAIN_ID_COLUMN[0]) {
        byte[] v = iterator.peekNext().getValue();
        String domainId = new String(v, UTF_8);
        entity.setDomainId(domainId);
      } else {
        LOG.warn(String.format("Found unexpected column for entity %s of "
            + "type %s (0x%02x)", entityId, entityType, key[prefixlen]));
      }
    }

    entity.setEntityId(entityId);
    entity.setEntityType(entityType);
    entity.setStartTime(startTime);

    return entity;
  }

  @Override
  public TimelineEvents getEntityTimelines(String entityType,
      SortedSet<String> entityIds, Long limit, Long windowStart,
      Long windowEnd, Set<String> eventType) throws IOException {
    TimelineEvents events = new TimelineEvents();
    if (entityIds == null || entityIds.isEmpty()) {
      return events;
    }
    Map<byte[], List<EntityIdentifier>> startTimeMap =
        new TreeMap<byte[], List<EntityIdentifier>>(
        new Comparator<byte[]>() {
          @Override
          public int compare(byte[] o1, byte[] o2) {
            return WritableComparator.compareBytes(o1, 0, o1.length, o2, 0,
                o2.length);
          }
        });
    DBIterator iterator = null;
    try {
      for (String entityId : entityIds) {
        byte[] startTime = getStartTime(entityId, entityType);
        if (startTime != null) {
          List<EntityIdentifier> entities = startTimeMap.get(startTime);
          if (entities == null) {
            entities = new ArrayList<EntityIdentifier>();
            startTimeMap.put(startTime, entities);
          }
          entities.add(new EntityIdentifier(entityId, entityType));
        }
      }
      for (Entry<byte[], List<EntityIdentifier>> entry : startTimeMap
          .entrySet()) {
        byte[] revStartTime = entry.getKey();
        for (EntityIdentifier entityIdentifier : entry.getValue()) {
          EventsOfOneEntity entity = new EventsOfOneEntity();
          entity.setEntityId(entityIdentifier.getId());
          entity.setEntityType(entityType);
          events.addEvent(entity);
          KeyBuilder kb = KeyBuilder.newInstance().add(entityType)
              .add(revStartTime).add(entityIdentifier.getId())
              .add(EVENTS_COLUMN);
          byte[] prefix = kb.getBytesForLookup();
          if (windowEnd == null) {
            windowEnd = Long.MAX_VALUE;
          }
          byte[] revts = writeReverseOrderedLong(windowEnd);
          kb.add(revts);
          byte[] first = kb.getBytesForLookup();
          byte[] last = null;
          if (windowStart != null) {
            last = KeyBuilder.newInstance().add(prefix)
                .add(writeReverseOrderedLong(windowStart)).getBytesForLookup();
          }
          if (limit == null) {
            limit = DEFAULT_LIMIT;
          }
          DB db = entitydb.getDBForStartTime(readReverseOrderedLong(
              revStartTime, 0));
          if (db == null) {
            continue;
          }
          iterator = db.iterator();
          for (iterator.seek(first); entity.getEvents().size() < limit
              && iterator.hasNext(); iterator.next()) {
            byte[] key = iterator.peekNext().getKey();
            if (!prefixMatches(prefix, prefix.length, key)
                || (last != null && WritableComparator.compareBytes(key, 0,
                    key.length, last, 0, last.length) > 0)) {
              break;
            }
            TimelineEvent event = getEntityEvent(eventType, key, prefix.length,
                iterator.peekNext().getValue());
            if (event != null) {
              entity.addEvent(event);
            }
          }
        }
      }
    } finally {
      IOUtils.cleanup(LOG, iterator);
    }
    return events;
  }

  @Override
  public TimelineEntities getEntities(String entityType, Long limit,
      Long windowStart, Long windowEnd, String fromId, Long fromTs,
      NameValuePair primaryFilter, Collection<NameValuePair> secondaryFilters,
      EnumSet<Field> fields, CheckAcl checkAcl) throws IOException {
    if (primaryFilter == null) {
      return getEntityByTime(EMPTY_BYTES, entityType, limit, windowStart,
          windowEnd, fromId, fromTs, secondaryFilters, fields, checkAcl, false);
    } else {
      byte[] base = KeyBuilder.newInstance().add(primaryFilter.getName())
          .add(fstConf.asByteArray(primaryFilter.getValue()), true)
          .getBytesForLookup();
      return getEntityByTime(base, entityType, limit, windowStart, windowEnd,
          fromId, fromTs, secondaryFilters, fields, checkAcl, true);
    }
  }

  private TimelineEntities getEntityByTime(byte[] base, String entityType,
      Long limit, Long starttime, Long endtime, String fromId, Long fromTs,
      Collection<NameValuePair> secondaryFilters, EnumSet<Field> fields,
      CheckAcl checkAcl, boolean usingPrimaryFilter) throws IOException {
    DBIterator iterator = null;
    try {
      KeyBuilder kb = KeyBuilder.newInstance().add(base).add(entityType);
      byte[] prefix = kb.getBytesForLookup();
      if (endtime == null) {
        endtime = Long.MAX_VALUE;
      }

      if (fields == null) {
        fields = EnumSet.allOf(Field.class);
      }

      long firstStartTime = Long.MAX_VALUE;
      byte[] first = null;
      if (fromId != null) {
        Long fromIdStartTime = getStartTimeLong(fromId, entityType);
        if (fromIdStartTime == null) {
          return new TimelineEntities();
        }
        if (fromIdStartTime <= endtime) {
          firstStartTime = fromIdStartTime;
          first = kb.add(writeReverseOrderedLong(fromIdStartTime)).add(fromId)
              .getBytesForLookup();
        }
      }
      if (first == null) {
        firstStartTime = endtime;
        first = kb.add(writeReverseOrderedLong(endtime)).getBytesForLookup();
      }
      byte[] last = null;
      if (starttime != null) {
        last = KeyBuilder.newInstance().add(base).add(entityType)
            .add(writeReverseOrderedLong(starttime)).getBytesForLookup();
      }
      if (limit == null) {
        limit = DEFAULT_LIMIT;
      }

      TimelineEntities entities = new TimelineEntities();
      RollingLevelDB rollingdb = null;
      if (usingPrimaryFilter) {
        rollingdb = indexdb;
      } else {
        rollingdb = entitydb;
      }

      DB db = rollingdb.getDBForStartTime(firstStartTime);
      while (entities.getEntities().size() < limit && db != null) {
        iterator = db.iterator();
        iterator.seek(first);

        while (entities.getEntities().size() < limit && iterator.hasNext()) {
          byte[] key = iterator.peekNext().getKey();
          if (!prefixMatches(prefix, prefix.length, key)
              || (last != null && WritableComparator.compareBytes(key, 0,
                  key.length, last, 0, last.length) > 0)) {
            break;
          }
          KeyParser kp = new KeyParser(key, prefix.length);
          Long startTime = kp.getNextLong();
          String entityId = kp.getNextString();

          if (fromTs != null) {
            long insertTime = readReverseOrderedLong(iterator.peekNext()
                .getValue(), 0);
            if (insertTime > fromTs) {
              byte[] firstKey = key;
              while (iterator.hasNext()) {
                key = iterator.peekNext().getKey();
                iterator.next();
                if (!prefixMatches(firstKey, kp.getOffset(), key)) {
                  break;
                }
              }
              continue;
            }
          }
          EnumSet<Field> queryFields = EnumSet.copyOf(fields);
          boolean addPrimaryFilters = false;
          boolean addOtherInfo = false;
          if (secondaryFilters != null && secondaryFilters.size() > 0) {
            if (!queryFields.contains(Field.PRIMARY_FILTERS)) {
              queryFields.add(Field.PRIMARY_FILTERS);
              addPrimaryFilters = true;
            }
            if (!queryFields.contains(Field.OTHER_INFO)) {
              queryFields.add(Field.OTHER_INFO);
              addOtherInfo = true;
            }
          }

          TimelineEntity entity = null;
          if (usingPrimaryFilter) {
            entity = getEntity(entityId, entityType, queryFields);
            iterator.next();
          } else {
            entity = getEntity(entityId, entityType, startTime, queryFields,
                iterator, key, kp.getOffset());
          }
          boolean filterPassed = true;
          if (secondaryFilters != null) {
            for (NameValuePair filter : secondaryFilters) {
              Object v = entity.getOtherInfo().get(filter.getName());
              if (v == null) {
                Set<Object> vs = entity.getPrimaryFilters()
                    .get(filter.getName());
                if (vs == null || !vs.contains(filter.getValue())) {
                  filterPassed = false;
                  break;
                }
              } else if (!v.equals(filter.getValue())) {
                filterPassed = false;
                break;
              }
            }
          }
          if (filterPassed) {
            if (entity.getDomainId() == null) {
              entity.setDomainId(DEFAULT_DOMAIN_ID);
            }
            if (checkAcl == null || checkAcl.check(entity)) {
              if (addPrimaryFilters) {
                entity.setPrimaryFilters(null);
              }
              if (addOtherInfo) {
                entity.setOtherInfo(null);
              }
              entities.addEntity(entity);
            }
          }
        }
        db = rollingdb.getPreviousDB(db);
      }
      return entities;
    } finally {
      IOUtils.cleanup(LOG, iterator);
    }
  }

  private long putEntities(TreeMap<Long, RollingWriteBatch> entityUpdates,
      TreeMap<Long, RollingWriteBatch> indexUpdates, TimelineEntity entity,
      TimelinePutResponse response) {

    long putCount = 0;
    List<EntityIdentifier> relatedEntitiesWithoutStartTimes =
        new ArrayList<EntityIdentifier>();
    byte[] revStartTime = null;
    Map<String, Set<Object>> primaryFilters = null;
    try {
      List<TimelineEvent> events = entity.getEvents();
      Long startTime = getAndSetStartTime(entity.getEntityId(),
          entity.getEntityType(), entity.getStartTime(), events);
      if (startTime == null) {
        TimelinePutError error = new TimelinePutError();
        error.setEntityId(entity.getEntityId());
        error.setEntityType(entity.getEntityType());
        error.setErrorCode(TimelinePutError.NO_START_TIME);
        response.addError(error);
        return putCount;
      }

      if (StringUtils.isEmpty(entity.getDomainId())) {
        TimelinePutError error = new TimelinePutError();
        error.setEntityId(entity.getEntityId());
        error.setEntityType(entity.getEntityType());
        error.setErrorCode(TimelinePutError.NO_DOMAIN);
        response.addError(error);
        return putCount;
      }

      revStartTime = writeReverseOrderedLong(startTime);
      long roundedStartTime = entitydb.computeCurrentCheckMillis(startTime);
      RollingWriteBatch rollingWriteBatch = entityUpdates.get(roundedStartTime);
      if (rollingWriteBatch == null) {
        DB db = entitydb.getDBForStartTime(startTime);
        if (db != null) {
          WriteBatch writeBatch = db.createWriteBatch();
          rollingWriteBatch = new RollingWriteBatch(db, writeBatch);
          entityUpdates.put(roundedStartTime, rollingWriteBatch);
        }
      }
      if (rollingWriteBatch == null) {
        TimelinePutError error = new TimelinePutError();
        error.setEntityId(entity.getEntityId());
        error.setEntityType(entity.getEntityType());
        error.setErrorCode(TimelinePutError.EXPIRED_ENTITY);
        response.addError(error);
        return putCount;
      }
      WriteBatch writeBatch = rollingWriteBatch.getWriteBatch();

      byte[] entityIdBytes = entity.getEntityId().getBytes(UTF_8);
      byte[] entityTypeBytes = entity.getEntityType().getBytes(UTF_8);
      byte[] domainIdBytes = entity.getDomainId().getBytes(UTF_8);

      byte[] markerKey = KeyBuilder.newInstance(3).add(entityTypeBytes, true)
          .add(revStartTime).add(entityIdBytes, true).getBytesForLookup();
      writeBatch.put(markerKey, EMPTY_BYTES);
      ++putCount;

      byte[] domainkey = KeyBuilder.newInstance(4).add(entityTypeBytes, true)
          .add(revStartTime).add(entityIdBytes, true).add(DOMAIN_ID_COLUMN)
          .getBytes();
      writeBatch.put(domainkey, domainIdBytes);
      ++putCount;

      if (events != null) {
        for (TimelineEvent event : events) {
          byte[] revts = writeReverseOrderedLong(event.getTimestamp());
          byte[] key = KeyBuilder.newInstance().add(entityTypeBytes, true)
              .add(revStartTime).add(entityIdBytes, true).add(EVENTS_COLUMN)
              .add(revts).add(event.getEventType().getBytes(UTF_8)).getBytes();
          byte[] value = fstConf.asByteArray(event.getEventInfo());
          writeBatch.put(key, value);
          ++putCount;
        }
      }

      primaryFilters = entity.getPrimaryFilters();
      if (primaryFilters != null) {
        for (Entry<String, Set<Object>> primaryFilter : primaryFilters
            .entrySet()) {
          for (Object primaryFilterValue : primaryFilter.getValue()) {
            byte[] key = KeyBuilder.newInstance(6).add(entityTypeBytes, true)
                .add(revStartTime).add(entityIdBytes, true)
                .add(PRIMARY_FILTERS_COLUMN).add(primaryFilter.getKey())
                .add(fstConf.asByteArray(primaryFilterValue)).getBytes();
            writeBatch.put(key, EMPTY_BYTES);
            ++putCount;
          }
        }
      }

      Map<String, Object> otherInfo = entity.getOtherInfo();
      if (otherInfo != null) {
        for (Entry<String, Object> info : otherInfo.entrySet()) {
          byte[] key = KeyBuilder.newInstance(5).add(entityTypeBytes, true)
              .add(revStartTime).add(entityIdBytes, true)
              .add(OTHER_INFO_COLUMN).add(info.getKey()).getBytes();
          byte[] value = fstConf.asByteArray(info.getValue());
          writeBatch.put(key, value);
          ++putCount;
        }
      }

      Map<String, Set<String>> relatedEntities = entity.getRelatedEntities();
      if (relatedEntities != null) {
        for (Entry<String, Set<String>> relatedEntityList : relatedEntities
            .entrySet()) {
          String relatedEntityType = relatedEntityList.getKey();
          for (String relatedEntityId : relatedEntityList.getValue()) {
            Long relatedStartTimeLong = getStartTimeLong(relatedEntityId,
                relatedEntityType);
            if (relatedStartTimeLong == null) {
              relatedEntitiesWithoutStartTimes.add(new EntityIdentifier(
                  relatedEntityId, relatedEntityType));
              continue;
            }

            byte[] relatedEntityStartTime =
                writeReverseOrderedLong(relatedStartTimeLong);
            long relatedRoundedStartTime = entitydb
                .computeCurrentCheckMillis(relatedStartTimeLong);
            RollingWriteBatch relatedRollingWriteBatch = entityUpdates
                .get(relatedRoundedStartTime);
            if (relatedRollingWriteBatch == null) {
              DB db = entitydb.getDBForStartTime(relatedStartTimeLong);
              if (db != null) {
                WriteBatch relatedWriteBatch = db.createWriteBatch();
                relatedRollingWriteBatch = new RollingWriteBatch(db,
                    relatedWriteBatch);
                entityUpdates.put(relatedRoundedStartTime,
                    relatedRollingWriteBatch);
              }
            }
            if (relatedRollingWriteBatch == null) {
              TimelinePutError error = new TimelinePutError();
              error.setEntityId(entity.getEntityId());
              error.setEntityType(entity.getEntityType());
              error.setErrorCode(TimelinePutError.EXPIRED_ENTITY);
              response.addError(error);
              continue;
            }
            byte[] relatedDomainIdBytes = relatedRollingWriteBatch.getDB().get(
                createDomainIdKey(relatedEntityId, relatedEntityType,
                    relatedEntityStartTime));
            String domainId = null;
            if (relatedDomainIdBytes == null) {
              domainId = TimelineDataManager.DEFAULT_DOMAIN_ID;
            } else {
              domainId = new String(relatedDomainIdBytes, UTF_8);
            }
            if (!domainId.equals(entity.getDomainId())) {
              TimelinePutError error = new TimelinePutError();
              error.setEntityId(entity.getEntityId());
              error.setEntityType(entity.getEntityType());
              error.setErrorCode(TimelinePutError.FORBIDDEN_RELATION);
              response.addError(error);
              continue;
            }
            byte[] key = createRelatedEntityKey(relatedEntityId,
                relatedEntityType, relatedEntityStartTime,
                entity.getEntityId(), entity.getEntityType());
            WriteBatch relatedWriteBatch = relatedRollingWriteBatch
                .getWriteBatch();
            relatedWriteBatch.put(key, EMPTY_BYTES);
            ++putCount;
          }
        }
      }

      RollingWriteBatch indexRollingWriteBatch = indexUpdates
          .get(roundedStartTime);
      if (indexRollingWriteBatch == null) {
        DB db = indexdb.getDBForStartTime(startTime);
        if (db != null) {
          WriteBatch indexWriteBatch = db.createWriteBatch();
          indexRollingWriteBatch = new RollingWriteBatch(db, indexWriteBatch);
          indexUpdates.put(roundedStartTime, indexRollingWriteBatch);
        }
      }
      if (indexRollingWriteBatch == null) {
        TimelinePutError error = new TimelinePutError();
        error.setEntityId(entity.getEntityId());
        error.setEntityType(entity.getEntityType());
        error.setErrorCode(TimelinePutError.EXPIRED_ENTITY);
        response.addError(error);
        return putCount;
      }
      WriteBatch indexWriteBatch = indexRollingWriteBatch.getWriteBatch();
      putCount += writePrimaryFilterEntries(indexWriteBatch, primaryFilters,
          markerKey, EMPTY_BYTES);
    } catch (IOException e) {
      LOG.error("Error putting entity " + entity.getEntityId() + " of type "
          + entity.getEntityType(), e);
      TimelinePutError error = new TimelinePutError();
      error.setEntityId(entity.getEntityId());
      error.setEntityType(entity.getEntityType());
      error.setErrorCode(TimelinePutError.IO_EXCEPTION);
      response.addError(error);
    }

    for (EntityIdentifier relatedEntity : relatedEntitiesWithoutStartTimes) {
      try {
        Long relatedEntityStartAndInsertTime = getAndSetStartTime(
            relatedEntity.getId(), relatedEntity.getType(),
            readReverseOrderedLong(revStartTime, 0), null);
        if (relatedEntityStartAndInsertTime == null) {
          throw new IOException("Error setting start time for related entity");
        }
        long relatedStartTimeLong = relatedEntityStartAndInsertTime;
        long relatedRoundedStartTime = entitydb
            .computeCurrentCheckMillis(relatedStartTimeLong);
        RollingWriteBatch relatedRollingWriteBatch = entityUpdates
            .get(relatedRoundedStartTime);
        if (relatedRollingWriteBatch == null) {
          DB db = entitydb.getDBForStartTime(relatedStartTimeLong);
          if (db != null) {
            WriteBatch relatedWriteBatch = db.createWriteBatch();
            relatedRollingWriteBatch = new RollingWriteBatch(db,
                relatedWriteBatch);
            entityUpdates
                .put(relatedRoundedStartTime, relatedRollingWriteBatch);
          }
        }
        if (relatedRollingWriteBatch == null) {
          TimelinePutError error = new TimelinePutError();
          error.setEntityId(entity.getEntityId());
          error.setEntityType(entity.getEntityType());
          error.setErrorCode(TimelinePutError.EXPIRED_ENTITY);
          response.addError(error);
          continue;
        }
        WriteBatch relatedWriteBatch = relatedRollingWriteBatch.getWriteBatch();
        byte[] relatedEntityStartTime =
            writeReverseOrderedLong(relatedEntityStartAndInsertTime);
        byte[] key = createDomainIdKey(relatedEntity.getId(),
            relatedEntity.getType(), relatedEntityStartTime);
        relatedWriteBatch.put(key, entity.getDomainId().getBytes(UTF_8));
        ++putCount;
        relatedWriteBatch.put(
            createRelatedEntityKey(relatedEntity.getId(),
                relatedEntity.getType(), relatedEntityStartTime,
                entity.getEntityId(), entity.getEntityType()), EMPTY_BYTES);
        ++putCount;
        relatedWriteBatch.put(
            createEntityMarkerKey(relatedEntity.getId(),
                relatedEntity.getType(), relatedEntityStartTime), EMPTY_BYTES);
        ++putCount;
      } catch (IOException e) {
        LOG.error(
            "Error putting related entity " + relatedEntity.getId()
                + " of type " + relatedEntity.getType() + " for entity "
                + entity.getEntityId() + " of type " + entity.getEntityType(),
            e);
        TimelinePutError error = new TimelinePutError();
        error.setEntityId(entity.getEntityId());
        error.setEntityType(entity.getEntityType());
        error.setErrorCode(TimelinePutError.IO_EXCEPTION);
        response.addError(error);
      }
    }

    return putCount;
  }

  private static long writePrimaryFilterEntries(WriteBatch writeBatch,
      Map<String, Set<Object>> primaryFilters, byte[] key, byte[] value)
      throws IOException {
    long putCount = 0;
    if (primaryFilters != null) {
      for (Entry<String, Set<Object>> pf : primaryFilters.entrySet()) {
        for (Object pfval : pf.getValue()) {
          writeBatch.put(addPrimaryFilterToKey(pf.getKey(), pfval, key), value);
          ++putCount;
        }
      }
    }
    return putCount;
  }

  @Override
  public TimelinePutResponse put(TimelineEntities entities) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Starting put");
    }
    TimelinePutResponse response = new TimelinePutResponse();
    TreeMap<Long, RollingWriteBatch> entityUpdates =
        new TreeMap<Long, RollingWriteBatch>();
    TreeMap<Long, RollingWriteBatch> indexUpdates =
        new TreeMap<Long, RollingWriteBatch>();

    long entityCount = 0;
    long indexCount = 0;

    try {

      for (TimelineEntity entity : entities.getEntities()) {
        entityCount += putEntities(entityUpdates, indexUpdates, entity,
            response);
      }

      for (RollingWriteBatch entityUpdate : entityUpdates.values()) {
        entityUpdate.write();
      }

      for (RollingWriteBatch indexUpdate : indexUpdates.values()) {
        indexUpdate.write();
      }

    } finally {

      for (RollingWriteBatch entityRollingWriteBatch : entityUpdates.values()) {
        entityRollingWriteBatch.close();
      }
      for (RollingWriteBatch indexRollingWriteBatch : indexUpdates.values()) {
        indexRollingWriteBatch.close();
      }
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Put " + entityCount + " new leveldb entity entries and "
          + indexCount + " new leveldb index entries from "
          + entities.getEntities().size() + " timeline entities");
    }
    return response;
  }

  private byte[] getStartTime(String entityId, String entityType)
      throws IOException {
    Long l = getStartTimeLong(entityId, entityType);
    return l == null ? null : writeReverseOrderedLong(l);
  }

  private Long getStartTimeLong(String entityId, String entityType)
      throws IOException {
    EntityIdentifier entity = new EntityIdentifier(entityId, entityType);
    if (startTimeReadCache.containsKey(entity)) {
      return startTimeReadCache.get(entity);
    } else {
      byte[] b = createStartTimeLookupKey(entity.getId(), entity.getType());
      byte[] v = starttimedb.get(b);
      if (v == null) {
        return null;
      } else {
        Long l = readReverseOrderedLong(v, 0);
        startTimeReadCache.put(entity, l);
        return l;
      }
    }
  }

  private Long getAndSetStartTime(String entityId, String entityType,
      Long startTime, List<TimelineEvent> events) throws IOException {
    EntityIdentifier entity = new EntityIdentifier(entityId, entityType);
    Long time = startTimeWriteCache.get(entity);
    if (time != null) {
      return time;
    }
    if (startTime == null && events != null) {
      startTime = Long.MAX_VALUE;
      for (TimelineEvent e : events) {
        if (e.getTimestamp() < startTime) {
          startTime = e.getTimestamp();
        }
      }
    }
    return checkStartTimeInDb(entity, startTime);
  }

  private Long checkStartTimeInDb(EntityIdentifier entity,
      Long suggestedStartTime) throws IOException {
    Long startAndInsertTime = null;
    byte[] b = createStartTimeLookupKey(entity.getId(), entity.getType());
    byte[] v = starttimedb.get(b);
    if (v == null) {
      if (suggestedStartTime == null) {
        return null;
      }
      startAndInsertTime = suggestedStartTime;

      starttimedb.put(b, writeReverseOrderedLong(suggestedStartTime));
    } else {
      startAndInsertTime = readReverseOrderedLong(v, 0);
    }
    startTimeWriteCache.put(entity, startAndInsertTime);
    startTimeReadCache.put(entity, startAndInsertTime);
    return startAndInsertTime;
  }

  private static byte[] createStartTimeLookupKey(String entityId,
      String entityType) throws IOException {
    return KeyBuilder.newInstance().add(entityType).add(entityId).getBytes();
  }

  private static byte[] createEntityMarkerKey(String entityId,
      String entityType, byte[] revStartTime) throws IOException {
    return KeyBuilder.newInstance().add(entityType).add(revStartTime)
        .add(entityId).getBytesForLookup();
  }

  private static byte[] addPrimaryFilterToKey(String primaryFilterName,
      Object primaryFilterValue, byte[] key) throws IOException {
    return KeyBuilder.newInstance().add(primaryFilterName)
        .add(fstConf.asByteArray(primaryFilterValue), true).add(key).getBytes();
  }

  private static TimelineEvent getEntityEvent(Set<String> eventTypes,
      byte[] key, int offset, byte[] value) throws IOException {
    KeyParser kp = new KeyParser(key, offset);
    long ts = kp.getNextLong();
    String tstype = kp.getNextString();
    if (eventTypes == null || eventTypes.contains(tstype)) {
      TimelineEvent event = new TimelineEvent();
      event.setTimestamp(ts);
      event.setEventType(tstype);
      Object o = fstConf.asObject(value);
      if (o == null) {
        event.setEventInfo(null);
      } else if (o instanceof Map) {
        @SuppressWarnings("unchecked")
        Map<String, Object> m = (Map<String, Object>) o;
        event.setEventInfo(m);
      } else {
        throw new IOException("Couldn't deserialize event info map");
      }
      return event;
    }
    return null;
  }

  private static void addPrimaryFilter(TimelineEntity entity, byte[] key,
      int offset) throws IOException {
    KeyParser kp = new KeyParser(key, offset);
    String name = kp.getNextString();
    byte[] bytes = kp.getRemainingBytes();
    Object value = fstConf.asObject(bytes);
    entity.addPrimaryFilter(name, value);
  }

  private static String parseRemainingKey(byte[] b, int offset) {
    return new String(b, offset, b.length - offset, UTF_8);
  }

  private static byte[] createRelatedEntityKey(String entityId,
      String entityType, byte[] revStartTime, String relatedEntityId,
      String relatedEntityType) throws IOException {
    return KeyBuilder.newInstance().add(entityType).add(revStartTime)
        .add(entityId).add(RELATED_ENTITIES_COLUMN).add(relatedEntityType)
        .add(relatedEntityId).getBytes();
  }

  private static void addRelatedEntity(TimelineEntity entity, byte[] key,
      int offset) throws IOException {
    KeyParser kp = new KeyParser(key, offset);
    String type = kp.getNextString();
    String id = kp.getNextString();
    entity.addRelatedEntity(type, id);
  }

  private static byte[] createDomainIdKey(String entityId, String entityType,
      byte[] revStartTime) throws IOException {
    return KeyBuilder.newInstance().add(entityType).add(revStartTime)
        .add(entityId).add(DOMAIN_ID_COLUMN).getBytes();
  }

  @VisibleForTesting
  void clearStartTimeCache() {
    startTimeWriteCache.clear();
    startTimeReadCache.clear();
  }

  @VisibleForTesting
  static int getStartTimeReadCacheSize(Configuration conf) {
    return conf
        .getInt(
            TIMELINE_SERVICE_LEVELDB_START_TIME_READ_CACHE_SIZE,
            DEFAULT_TIMELINE_SERVICE_LEVELDB_START_TIME_READ_CACHE_SIZE);
  }

  @VisibleForTesting
  static int getStartTimeWriteCacheSize(Configuration conf) {
    return conf
        .getInt(
            TIMELINE_SERVICE_LEVELDB_START_TIME_WRITE_CACHE_SIZE,
            DEFAULT_TIMELINE_SERVICE_LEVELDB_START_TIME_WRITE_CACHE_SIZE);
  }

  @VisibleForTesting
  long evictOldStartTimes(long minStartTime) throws IOException {
    LOG.info("Searching for start times to evict earlier than " + minStartTime);

    long batchSize = 0;
    long totalCount = 0;
    long startTimesCount = 0;

    WriteBatch writeBatch = null;
    DBIterator iterator = null;

    try {
      writeBatch = starttimedb.createWriteBatch();
      ReadOptions readOptions = new ReadOptions();
      readOptions.fillCache(false);
      iterator = starttimedb.iterator(readOptions);
      iterator.seekToFirst();

      while (iterator.hasNext()) {
        Map.Entry<byte[], byte[]> current = iterator.next();
        byte[] entityKey = current.getKey();
        byte[] entityValue = current.getValue();
        long startTime = readReverseOrderedLong(entityValue, 0);
        if (startTime < minStartTime) {
          ++batchSize;
          ++startTimesCount;
          writeBatch.delete(entityKey);

          if (batchSize >= writeBatchSize) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Preparing to delete a batch of " + batchSize
                  + " old start times");
            }
            starttimedb.write(writeBatch);
            if (LOG.isDebugEnabled()) {
              LOG.debug("Deleted batch of " + batchSize
                  + ". Total start times deleted so far this cycle: "
                  + startTimesCount);
            }
            IOUtils.cleanup(LOG, writeBatch);
            writeBatch = starttimedb.createWriteBatch();
            batchSize = 0;
          }
        }
        ++totalCount;
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Preparing to delete a batch of " + batchSize
            + " old start times");
      }
      starttimedb.write(writeBatch);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Deleted batch of " + batchSize
            + ". Total start times deleted so far this cycle: "
            + startTimesCount);
      }
      LOG.info("Deleted " + startTimesCount + "/" + totalCount
          + " start time entities earlier than " + minStartTime);
    } finally {
      IOUtils.cleanup(LOG, writeBatch);
      IOUtils.cleanup(LOG, iterator);
    }
    return startTimesCount;
  }

  @VisibleForTesting
  void discardOldEntities(long timestamp) throws IOException,
      InterruptedException {
    long totalCount = 0;
    long t1 = System.currentTimeMillis();
    try {
      totalCount += evictOldStartTimes(timestamp);
      indexdb.evictOldDBs();
      entitydb.evictOldDBs();
    } finally {
      long t2 = System.currentTimeMillis();
      LOG.info("Discarded " + totalCount + " entities for timestamp "
          + timestamp + " and earlier in " + (t2 - t1) / 1000.0 + " seconds");
    }
  }

  Version loadVersion() throws IOException {
    byte[] data = starttimedb.get(bytes(TIMELINE_STORE_VERSION_KEY));
    if (data == null || data.length == 0) {
      return Version.newInstance(1, 0);
    }
    Version version = new VersionPBImpl(VersionProto.parseFrom(data));
    return version;
  }

  @VisibleForTesting
  void storeVersion(Version state) throws IOException {
    dbStoreVersion(state);
  }

  private void dbStoreVersion(Version state) throws IOException {
    String key = TIMELINE_STORE_VERSION_KEY;
    byte[] data = ((VersionPBImpl) state).getProto().toByteArray();
    try {
      starttimedb.put(bytes(key), data);
    } catch (DBException e) {
      throw new IOException(e);
    }
  }

  Version getCurrentVersion() {
    return CURRENT_VERSION_INFO;
  }

  private void checkVersion() throws IOException {
    Version loadedVersion = loadVersion();
    LOG.info("Loaded timeline store version info " + loadedVersion);
    if (loadedVersion.equals(getCurrentVersion())) {
      return;
    }
    if (loadedVersion.isCompatibleTo(getCurrentVersion())) {
      LOG.info("Storing timeline store version info " + getCurrentVersion());
      dbStoreVersion(CURRENT_VERSION_INFO);
    } else {
      String incompatibleMessage = "Incompatible version for timeline store: "
          + "expecting version " + getCurrentVersion()
          + ", but loading version " + loadedVersion;
      LOG.fatal(incompatibleMessage);
      throw new IOException(incompatibleMessage);
    }
  }

  @Override
  public void put(TimelineDomain domain) throws IOException {
    WriteBatch domainWriteBatch = null;
    WriteBatch ownerWriteBatch = null;
    try {
      domainWriteBatch = domaindb.createWriteBatch();
      ownerWriteBatch = ownerdb.createWriteBatch();
      if (domain.getId() == null || domain.getId().length() == 0) {
        throw new IllegalArgumentException("Domain doesn't have an ID");
      }
      if (domain.getOwner() == null || domain.getOwner().length() == 0) {
        throw new IllegalArgumentException("Domain doesn't have an owner.");
      }

      byte[] domainEntryKey = createDomainEntryKey(domain.getId(),
          DESCRIPTION_COLUMN);
      byte[] ownerLookupEntryKey = createOwnerLookupKey(domain.getOwner(),
          domain.getId(), DESCRIPTION_COLUMN);
      if (domain.getDescription() != null) {
        domainWriteBatch.put(domainEntryKey,
            domain.getDescription().getBytes(UTF_8));
        ownerWriteBatch.put(ownerLookupEntryKey, domain.getDescription()
            .getBytes(UTF_8));
      } else {
        domainWriteBatch.put(domainEntryKey, EMPTY_BYTES);
        ownerWriteBatch.put(ownerLookupEntryKey, EMPTY_BYTES);
      }

      domainEntryKey = createDomainEntryKey(domain.getId(), OWNER_COLUMN);
      ownerLookupEntryKey = createOwnerLookupKey(domain.getOwner(),
          domain.getId(), OWNER_COLUMN);
      domainWriteBatch.put(domainEntryKey, domain.getOwner().getBytes(UTF_8));
      ownerWriteBatch.put(ownerLookupEntryKey, domain.getOwner()
          .getBytes(UTF_8));

      domainEntryKey = createDomainEntryKey(domain.getId(), READER_COLUMN);
      ownerLookupEntryKey = createOwnerLookupKey(domain.getOwner(),
          domain.getId(), READER_COLUMN);
      if (domain.getReaders() != null && domain.getReaders().length() > 0) {
        domainWriteBatch.put(domainEntryKey, domain.getReaders()
            .getBytes(UTF_8));
        ownerWriteBatch.put(ownerLookupEntryKey,
            domain.getReaders().getBytes(UTF_8));
      } else {
        domainWriteBatch.put(domainEntryKey, EMPTY_BYTES);
        ownerWriteBatch.put(ownerLookupEntryKey, EMPTY_BYTES);
      }

      domainEntryKey = createDomainEntryKey(domain.getId(), WRITER_COLUMN);
      ownerLookupEntryKey = createOwnerLookupKey(domain.getOwner(),
          domain.getId(), WRITER_COLUMN);
      if (domain.getWriters() != null && domain.getWriters().length() > 0) {
        domainWriteBatch.put(domainEntryKey, domain.getWriters()
            .getBytes(UTF_8));
        ownerWriteBatch.put(ownerLookupEntryKey,
            domain.getWriters().getBytes(UTF_8));
      } else {
        domainWriteBatch.put(domainEntryKey, EMPTY_BYTES);
        ownerWriteBatch.put(ownerLookupEntryKey, EMPTY_BYTES);
      }

      domainEntryKey = createDomainEntryKey(domain.getId(), TIMESTAMP_COLUMN);
      ownerLookupEntryKey = createOwnerLookupKey(domain.getOwner(),
          domain.getId(), TIMESTAMP_COLUMN);
      long currentTimestamp = System.currentTimeMillis();
      byte[] timestamps = domaindb.get(domainEntryKey);
      if (timestamps == null) {
        timestamps = new byte[16];
        writeReverseOrderedLong(currentTimestamp, timestamps, 0);
        writeReverseOrderedLong(currentTimestamp, timestamps, 8);
      } else {
        writeReverseOrderedLong(currentTimestamp, timestamps, 8);
      }
      domainWriteBatch.put(domainEntryKey, timestamps);
      ownerWriteBatch.put(ownerLookupEntryKey, timestamps);
      domaindb.write(domainWriteBatch);
      ownerdb.write(ownerWriteBatch);
    } finally {
      IOUtils.cleanup(LOG, domainWriteBatch);
      IOUtils.cleanup(LOG, ownerWriteBatch);
    }
  }

  private static byte[] createDomainEntryKey(String domainId, byte[] columnName)
      throws IOException {
    return KeyBuilder.newInstance().add(domainId).add(columnName).getBytes();
  }

  private static byte[] createOwnerLookupKey(String owner, String domainId,
      byte[] columnName) throws IOException {
    return KeyBuilder.newInstance().add(owner).add(domainId).add(columnName)
        .getBytes();
  }

  @Override
  public TimelineDomain getDomain(String domainId) throws IOException {
    DBIterator iterator = null;
    try {
      byte[] prefix = KeyBuilder.newInstance().add(domainId)
          .getBytesForLookup();
      iterator = domaindb.iterator();
      iterator.seek(prefix);
      return getTimelineDomain(iterator, domainId, prefix);
    } finally {
      IOUtils.cleanup(LOG, iterator);
    }
  }

  @Override
  public TimelineDomains getDomains(String owner) throws IOException {
    DBIterator iterator = null;
    try {
      byte[] prefix = KeyBuilder.newInstance().add(owner).getBytesForLookup();
      List<TimelineDomain> domains = new ArrayList<TimelineDomain>();
      for (iterator = ownerdb.iterator(), iterator.seek(prefix); iterator
          .hasNext();) {
        byte[] key = iterator.peekNext().getKey();
        if (!prefixMatches(prefix, prefix.length, key)) {
          break;
        }
        KeyParser kp = new KeyParser(key, prefix.length);
        String domainId = kp.getNextString();
        byte[] prefixExt = KeyBuilder.newInstance().add(owner).add(domainId)
            .getBytesForLookup();
        TimelineDomain domainToReturn = getTimelineDomain(iterator, domainId,
            prefixExt);
        if (domainToReturn != null) {
          domains.add(domainToReturn);
        }
      }
      Collections.sort(domains, new Comparator<TimelineDomain>() {
        @Override
        public int compare(TimelineDomain domain1, TimelineDomain domain2) {
          int result = domain2.getCreatedTime().compareTo(
              domain1.getCreatedTime());
          if (result == 0) {
            return domain2.getModifiedTime().compareTo(
                domain1.getModifiedTime());
          } else {
            return result;
          }
        }
      });
      TimelineDomains domainsToReturn = new TimelineDomains();
      domainsToReturn.addDomains(domains);
      return domainsToReturn;
    } finally {
      IOUtils.cleanup(LOG, iterator);
    }
  }

  private static TimelineDomain getTimelineDomain(DBIterator iterator,
      String domainId, byte[] prefix) throws IOException {
    TimelineDomain domain = new TimelineDomain();
    domain.setId(domainId);
    boolean noRows = true;
    for (; iterator.hasNext(); iterator.next()) {
      byte[] key = iterator.peekNext().getKey();
      if (!prefixMatches(prefix, prefix.length, key)) {
        break;
      }
      if (noRows) {
        noRows = false;
      }
      byte[] value = iterator.peekNext().getValue();
      if (value != null && value.length > 0) {
        if (key[prefix.length] == DESCRIPTION_COLUMN[0]) {
          domain.setDescription(new String(value, UTF_8));
        } else if (key[prefix.length] == OWNER_COLUMN[0]) {
          domain.setOwner(new String(value, UTF_8));
        } else if (key[prefix.length] == READER_COLUMN[0]) {
          domain.setReaders(new String(value, UTF_8));
        } else if (key[prefix.length] == WRITER_COLUMN[0]) {
          domain.setWriters(new String(value, UTF_8));
        } else if (key[prefix.length] == TIMESTAMP_COLUMN[0]) {
          domain.setCreatedTime(readReverseOrderedLong(value, 0));
          domain.setModifiedTime(readReverseOrderedLong(value, 8));
        } else {
          LOG.error("Unrecognized domain column: " + key[prefix.length]);
        }
      }
    }
    if (noRows) {
      return null;
    } else {
      return domain;
    }
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-applicationhistoryservice/src/main/java/org/apache/hadoop/yarn/server/timeline/TimelineDataManager.java

package org.apache.hadoop.yarn.server.timeline;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.timeline.TimelineReader.Field;
import org.apache.hadoop.yarn.server.timeline.security.TimelineACLsManager;

import com.google.common.annotations.VisibleForTesting;

            eventsItr.remove();
          }
        } catch (Exception e) {
          LOG.warn("Error when verifying access for user " + callerUGI
              + " on the events of the timeline entity "
              + new EntityIdentifier(eventsOfOneEntity.getEntityId(),
                  eventsOfOneEntity.getEntityType()), e);
    if (entities == null) {
      return new TimelinePutResponse();
    }
    TimelineEntities entitiesToPut = new TimelineEntities();
    List<TimelinePutResponse.TimelinePutError> errors =
        new ArrayList<TimelinePutResponse.TimelinePutError>();
    for (TimelineEntity entity : entities.getEntities()) {

      TimelineEntity existingEntity = null;
      try {
        existingEntity =
            store.getEntity(entity.getEntityId(), entity.getEntityType(),
                EnumSet.of(Field.PRIMARY_FILTERS));
        if (existingEntity != null) {
          addDefaultDomainIdIfAbsent(existingEntity);
          if (!existingEntity.getDomainId().equals(entity.getDomainId())) {
            throw new YarnException("The domain of the timeline entity "
              + "{ id: " + entity.getEntityId() + ", type: "
              + entity.getEntityType() + " } is not allowed to be changed from "
              + existingEntity.getDomainId() + " to " + entity.getDomainId());
          }
        }
        if (!timelineACLsManager.checkAccess(
            callerUGI, ApplicationAccessType.MODIFY_APP, entity)) {
          throw new YarnException(callerUGI
              + " is not allowed to put the timeline entity "
              + "{ id: " + entity.getEntityId() + ", type: "
              + entity.getEntityType() + " } into the domain "
              + entity.getDomainId() + ".");
        }
      } catch (Exception e) {
        LOG.warn("Skip the timeline entity: { id: " + entity.getEntityId()
            + ", type: "+ entity.getEntityType() + " }", e);
        TimelinePutResponse.TimelinePutError error =
            new TimelinePutResponse.TimelinePutError();
        error.setEntityId(entity.getEntityId());
        error.setEntityType(entity.getEntityType());
        error.setErrorCode(
            TimelinePutResponse.TimelinePutError.ACCESS_DENIED);
        errors.add(error);
        continue;
      }

      entitiesToPut.addEntity(entity);
    }

    TimelinePutResponse response = store.put(entitiesToPut);
    response.addErrors(errors);

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-applicationhistoryservice/src/main/java/org/apache/hadoop/yarn/server/timeline/util/LeveldbUtils.java

import org.apache.hadoop.io.WritableComparator;

import java.io.IOException;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.yarn.server.timeline.GenericObjectMapper.readReverseOrderedLong;

public class LeveldbUtils {

  public static class KeyBuilder {
    private static final int MAX_NUMBER_OF_KEY_ELEMENTS = 10;
    private byte[][] b;
    private boolean[] useSeparator;
      return new KeyBuilder(MAX_NUMBER_OF_KEY_ELEMENTS);
    }

    public static KeyBuilder newInstance(final int size) {
      return new KeyBuilder(size);
    }

    public KeyBuilder add(String s) {
      return add(s.getBytes(UTF_8), true);
    }

    public KeyBuilder add(byte[] t) {
      return this;
    }

    public byte[] getBytes() {
      int bytesLength = length;
      if (useSeparator[index - 1]) {
        bytesLength = length - 1;
      }
      byte[] bytes = new byte[bytesLength];
      int curPos = 0;
      for (int i = 0; i < index; i++) {
        System.arraycopy(b[i], 0, bytes, curPos, b[i].length);
        curPos += b[i].length;
        if (i < index - 1 && useSeparator[i]) {
          bytes[curPos++] = 0x0;
        }
      }
      return bytes;
    }

    public byte[] getBytesForLookup() {
      byte[] bytes = new byte[length];
      int curPos = 0;
      for (int i = 0; i < index; i++) {
        System.arraycopy(b[i], 0, bytes, curPos, b[i].length);
        curPos += b[i].length;
        if (useSeparator[i]) {
          bytes[curPos++] = 0x0;
        }
      }
      return bytes;
    }
  }

    private final byte[] b;
    private int offset;

    public KeyParser(final byte[] b, final int offset) {
      this.b = b;
      this.offset = offset;
    }

    public String getNextString() throws IOException {
      if (offset >= b.length) {
        throw new IOException(
      while (offset + i < b.length && b[offset + i] != 0x0) {
        i++;
      }
      String s = new String(b, offset, i, UTF_8);
      offset = offset + i + 1;
      return s;
    }

    public void skipNextString() throws IOException {
      if (offset >= b.length) {
        throw new IOException("tried to read nonexistent string from byte array");
      }
      while (offset < b.length && b[offset] != 0x0) {
        ++offset;
      }
      ++offset;
    }

    public long getNextLong() throws IOException {
      if (offset + 8 >= b.length) {
        throw new IOException("byte array ran out when trying to read long");
      }
      long value = readReverseOrderedLong(b, offset);
      offset += 8;
      return value;
    }

    public int getOffset() {
      return offset;
    }

    public byte[] getRemainingBytes() {
      byte[] bytes = new byte[b.length - offset];
      System.arraycopy(b, offset, bytes, 0, b.length - offset);
      return bytes;
    }
  }


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-applicationhistoryservice/src/test/java/org/apache/hadoop/yarn/server/timeline/TestLeveldbTimelineStore.java
import org.apache.hadoop.yarn.api.records.timeline.TimelinePutResponse.TimelinePutError;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.records.Version;
import org.apache.hadoop.yarn.server.utils.LeveldbIterator;
import org.iq80.leveldb.DBException;
import org.junit.After;
    assertEquals(1, getEntities("type_2").size());

    assertEquals(false, deleteNextEntity(entityType1,
        writeReverseOrderedLong(60L)));
    assertEquals(3, getEntities("type_1").size());
    assertEquals(1, getEntities("type_2").size());

    assertEquals(true, deleteNextEntity(entityType1,
        writeReverseOrderedLong(123L)));
    List<TimelineEntity> entities = getEntities("type_2");
    assertEquals(1, entities.size());
    verifyEntityInfo(entityId2, entityType2, events2, Collections.singletonMap(
    verifyEntityInfo(entityId6, entityType1, EMPTY_EVENTS, EMPTY_REL_ENTITIES,
        primaryFilters, otherInfo, entities.get(1), domainId2);

    ((LeveldbTimelineStore)store).discardOldEntities(0L);
    assertEquals(2, getEntities("type_1").size());
    assertEquals(0, getEntities("type_2").size());
    assertEquals(6, ((LeveldbTimelineStore)store).getEntityTypes().size());

    ((LeveldbTimelineStore)store).discardOldEntities(123L);
    assertEquals(0, getEntities("type_1").size());
    assertEquals(0, getEntities("type_2").size());
    assertEquals(0, ((LeveldbTimelineStore)store).getEntityTypes().size());
    verifyEntityInfo(entityId6, entityType1, EMPTY_EVENTS, EMPTY_REL_ENTITIES,
        primaryFilters, otherInfo, entities.get(2), domainId2);

    ((LeveldbTimelineStore)store).discardOldEntities(-123L);
    assertEquals(1, getEntitiesWithPrimaryFilter("type_1", pfPair).size());
    assertEquals(3, getEntitiesWithPrimaryFilter("type_1", userFilter).size());

    ((LeveldbTimelineStore)store).discardOldEntities(123L);
    assertEquals(0, getEntities("type_1").size());
    assertEquals(0, getEntities("type_2").size());
    assertEquals(0, ((LeveldbTimelineStore)store).getEntityTypes().size());
    assertEquals(1, getEntitiesFromTs("type_2", l).size());
    assertEquals(3, getEntitiesFromTsWithPrimaryFilter("type_1", userFilter,
        l).size());
    ((LeveldbTimelineStore)store).discardOldEntities(123L);
    assertEquals(0, getEntitiesFromTs("type_1", l).size());
    assertEquals(0, getEntitiesFromTs("type_2", l).size());
    assertEquals(0, getEntitiesFromTsWithPrimaryFilter("type_1", userFilter,
    Assert.assertEquals(defaultVersion, dbStore.loadVersion());

    Version incompatibleVersion = Version.newInstance(
        defaultVersion.getMajorVersion() + 1, defaultVersion.getMinorVersion());
    dbStore.storeVersion(incompatibleVersion);
    try {
      restartTimelineStore();

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-applicationhistoryservice/src/test/java/org/apache/hadoop/yarn/server/timeline/TestRollingLevelDB.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-applicationhistoryservice/src/test/java/org/apache/hadoop/yarn/server/timeline/TestRollingLevelDB.java
package org.apache.hadoop.yarn.server.timeline;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.iq80.leveldb.DB;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestRollingLevelDB {
  private Configuration conf = new YarnConfiguration();
  private FileSystem lfs;
  private MyRollingLevelDB rollingLevelDB;

  public static class MyRollingLevelDB extends RollingLevelDB {
    private long currentTimeMillis;

    MyRollingLevelDB() {
      super("Test");
      this.currentTimeMillis = System.currentTimeMillis();
    }

    @Override
    protected long currentTimeMillis() {
      return currentTimeMillis;
    }

    public void setCurrentTimeMillis(long time) {
      this.currentTimeMillis = time;
    }
  };

  @Before
  public void setup() throws Exception {
    lfs = FileSystem.getLocal(conf);
    File fsPath = new File("target", this.getClass().getSimpleName() +
        "-tmpDir").getAbsoluteFile();
    conf.set(YarnConfiguration.TIMELINE_SERVICE_LEVELDB_PATH,
        fsPath.getAbsolutePath());
    lfs.delete(new Path(fsPath.getAbsolutePath()), true);
    rollingLevelDB = new MyRollingLevelDB();
  }

  @Test
  public void testInsertAfterRollPeriodRollsDB() throws Exception {

    rollingLevelDB.init(conf);
    long now = rollingLevelDB.currentTimeMillis();
    DB db = rollingLevelDB.getDBForStartTime(now);
    long startTime = rollingLevelDB.getStartTimeFor(db);
    Assert.assertEquals("Received level db for incorrect start time",
        rollingLevelDB.computeCurrentCheckMillis(now),
        startTime);
    now = rollingLevelDB.getNextRollingTimeMillis();
    rollingLevelDB.setCurrentTimeMillis(now);
    db = rollingLevelDB.getDBForStartTime(now);
    startTime = rollingLevelDB.getStartTimeFor(db);
    Assert.assertEquals("Received level db for incorrect start time",
        rollingLevelDB.computeCurrentCheckMillis(now),
        startTime);
  }

  @Test
  public void testInsertForPreviousPeriodAfterRollPeriodRollsDB()
      throws Exception {

    rollingLevelDB.init(conf);
    long now = rollingLevelDB.currentTimeMillis();
    now = rollingLevelDB.computeCurrentCheckMillis(now);
    rollingLevelDB.setCurrentTimeMillis(now);
    DB db = rollingLevelDB.getDBForStartTime(now - 1);
    long startTime = rollingLevelDB.getStartTimeFor(db);
    Assert.assertEquals("Received level db for incorrect start time",
        rollingLevelDB.computeCurrentCheckMillis(now - 1),
        startTime);
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-applicationhistoryservice/src/test/java/org/apache/hadoop/yarn/server/timeline/TestRollingLevelDBTimelineStore.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-applicationhistoryservice/src/test/java/org/apache/hadoop/yarn/server/timeline/TestRollingLevelDBTimelineStore.java
package org.apache.hadoop.yarn.server.timeline;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.service.ServiceStateException;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEvent;
import org.apache.hadoop.yarn.api.records.timeline.TimelinePutResponse;
import org.apache.hadoop.yarn.api.records.timeline.TimelinePutResponse.TimelinePutError;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.records.Version;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mortbay.log.Log;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class TestRollingLevelDBTimelineStore extends TimelineStoreTestUtils {
  private FileContext fsContext;
  private File fsPath;
  private Configuration config = new YarnConfiguration();

  @Before
  public void setup() throws Exception {
    fsContext = FileContext.getLocalFSFileContext();
    fsPath = new File("target", this.getClass().getSimpleName() +
        "-tmpDir").getAbsoluteFile();
    fsContext.delete(new Path(fsPath.getAbsolutePath()), true);
    config.set(YarnConfiguration.TIMELINE_SERVICE_LEVELDB_PATH,
        fsPath.getAbsolutePath());
    config.setBoolean(YarnConfiguration.TIMELINE_SERVICE_TTL_ENABLE, false);
    store = new RollingLevelDBTimelineStore();
    store.init(config);
    store.start();
    loadTestEntityData();
    loadVerificationEntityData();
    loadTestDomainData();
  }

  @After
  public void tearDown() throws Exception {
    store.stop();
    fsContext.delete(new Path(fsPath.getAbsolutePath()), true);
  }

  @Test
  public void testRootDirPermission() throws IOException {
    FileSystem fs = FileSystem.getLocal(new YarnConfiguration());
    FileStatus file = fs.getFileStatus(new Path(fsPath.getAbsolutePath(),
        RollingLevelDBTimelineStore.FILENAME));
    assertNotNull(file);
    assertEquals(RollingLevelDBTimelineStore.LEVELDB_DIR_UMASK,
        file.getPermission());
  }

  @Test
  public void testGetSingleEntity() throws IOException {
    super.testGetSingleEntity();
    ((RollingLevelDBTimelineStore)store).clearStartTimeCache();
    super.testGetSingleEntity();
    loadTestEntityData();
  }

  @Test
  public void testGetEntities() throws IOException {
    super.testGetEntities();
  }

  @Test
  public void testGetEntitiesWithFromId() throws IOException {
    super.testGetEntitiesWithFromId();
  }

  @Test
  public void testGetEntitiesWithFromTs() throws IOException {
  }

  @Test
  public void testGetEntitiesWithPrimaryFilters() throws IOException {
    super.testGetEntitiesWithPrimaryFilters();
  }

  @Test
  public void testGetEntitiesWithSecondaryFilters() throws IOException {
    super.testGetEntitiesWithSecondaryFilters();
  }

  @Test
  public void testGetEvents() throws IOException {
    super.testGetEvents();
  }

  @Test
  public void testCacheSizes() {
    Configuration conf = new Configuration();
    assertEquals(10000,
        RollingLevelDBTimelineStore.getStartTimeReadCacheSize(conf));
    assertEquals(10000,
        RollingLevelDBTimelineStore.getStartTimeWriteCacheSize(conf));
    conf.setInt(
        YarnConfiguration.TIMELINE_SERVICE_LEVELDB_START_TIME_READ_CACHE_SIZE,
        10001);
    assertEquals(10001,
        RollingLevelDBTimelineStore.getStartTimeReadCacheSize(conf));
    conf = new Configuration();
    conf.setInt(
        YarnConfiguration.TIMELINE_SERVICE_LEVELDB_START_TIME_WRITE_CACHE_SIZE,
        10002);
    assertEquals(10002,
        RollingLevelDBTimelineStore.getStartTimeWriteCacheSize(conf));
  }

  @Test
  public void testCheckVersion() throws IOException {
    RollingLevelDBTimelineStore dbStore = (RollingLevelDBTimelineStore) store;
    Version defaultVersion = dbStore.getCurrentVersion();
    Assert.assertEquals(defaultVersion, dbStore.loadVersion());

    Version compatibleVersion =
        Version.newInstance(defaultVersion.getMajorVersion(),
          defaultVersion.getMinorVersion() + 2);
    dbStore.storeVersion(compatibleVersion);
    Assert.assertEquals(compatibleVersion, dbStore.loadVersion());
    restartTimelineStore();
    dbStore = (RollingLevelDBTimelineStore) store;
    Assert.assertEquals(defaultVersion, dbStore.loadVersion());

    Version incompatibleVersion =
        Version.newInstance(defaultVersion.getMajorVersion() + 1,
          defaultVersion.getMinorVersion());
    dbStore.storeVersion(incompatibleVersion);
    try {
      restartTimelineStore();
      Assert.fail("Incompatible version, should expect fail here.");
    } catch (ServiceStateException e) {
      Assert.assertTrue("Exception message mismatch",
          e.getMessage().contains("Incompatible version for timeline store"));
    }
  }

  @Test
  public void testValidateConfig() throws IOException {
    Configuration copyConfig = new YarnConfiguration(config);
    try {
      Configuration newConfig = new YarnConfiguration(copyConfig);
      newConfig.setLong(YarnConfiguration.TIMELINE_SERVICE_TTL_MS, 0);
      config = newConfig;
      restartTimelineStore();
      Assert.fail();
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().contains(
          YarnConfiguration.TIMELINE_SERVICE_TTL_MS));
    }
    try {
      Configuration newConfig = new YarnConfiguration(copyConfig);
      newConfig.setLong(
          YarnConfiguration.TIMELINE_SERVICE_LEVELDB_TTL_INTERVAL_MS, 0);
      config = newConfig;
      restartTimelineStore();
      Assert.fail();
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().contains(
          YarnConfiguration.TIMELINE_SERVICE_LEVELDB_TTL_INTERVAL_MS));
    }
    try {
      Configuration newConfig = new YarnConfiguration(copyConfig);
      newConfig.setLong(
          YarnConfiguration.TIMELINE_SERVICE_LEVELDB_READ_CACHE_SIZE, -1);
      config = newConfig;
      restartTimelineStore();
      Assert.fail();
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().contains(
          YarnConfiguration.TIMELINE_SERVICE_LEVELDB_READ_CACHE_SIZE));
    }
    try {
      Configuration newConfig = new YarnConfiguration(copyConfig);
      newConfig.setLong(
          YarnConfiguration.TIMELINE_SERVICE_LEVELDB_START_TIME_READ_CACHE_SIZE,
          0);
      config = newConfig;
      restartTimelineStore();
      Assert.fail();
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().contains(
          YarnConfiguration
          .TIMELINE_SERVICE_LEVELDB_START_TIME_READ_CACHE_SIZE));
    }
    try {
      Configuration newConfig = new YarnConfiguration(copyConfig);
      newConfig.setLong(
          YarnConfiguration
          .TIMELINE_SERVICE_LEVELDB_START_TIME_WRITE_CACHE_SIZE,
          0);
      config = newConfig;
      restartTimelineStore();
      Assert.fail();
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().contains(
          YarnConfiguration
          .TIMELINE_SERVICE_LEVELDB_START_TIME_WRITE_CACHE_SIZE));
    }
    config = copyConfig;
    restartTimelineStore();
  }

  private void restartTimelineStore() throws IOException {
    if (store != null) {
      store.close();
    }
    store = new RollingLevelDBTimelineStore();
    store.init(config);
    store.start();
  }

  @Test
  public void testGetDomain() throws IOException {
    super.testGetDomain();
  }

  @Test
  public void testGetDomains() throws IOException {
    super.testGetDomains();
  }

  @Test
  public void testRelatingToNonExistingEntity() throws IOException {
    TimelineEntity entityToStore = new TimelineEntity();
    entityToStore.setEntityType("TEST_ENTITY_TYPE_1");
    entityToStore.setEntityId("TEST_ENTITY_ID_1");
    entityToStore.setDomainId(TimelineDataManager.DEFAULT_DOMAIN_ID);
    entityToStore.addRelatedEntity("TEST_ENTITY_TYPE_2", "TEST_ENTITY_ID_2");
    TimelineEntities entities = new TimelineEntities();
    entities.addEntity(entityToStore);
    store.put(entities);
    TimelineEntity entityToGet =
        store.getEntity("TEST_ENTITY_ID_2", "TEST_ENTITY_TYPE_2", null);
    Assert.assertNotNull(entityToGet);
    Assert.assertEquals("DEFAULT", entityToGet.getDomainId());
    Assert.assertEquals("TEST_ENTITY_TYPE_1",
        entityToGet.getRelatedEntities().keySet().iterator().next());
    Assert.assertEquals("TEST_ENTITY_ID_1",
        entityToGet.getRelatedEntities().values().iterator().next()
            .iterator().next());
  }

  @Test
  public void testRelatingToEntityInSamePut() throws IOException {
    TimelineEntity entityToRelate = new TimelineEntity();
    entityToRelate.setEntityType("TEST_ENTITY_TYPE_2");
    entityToRelate.setEntityId("TEST_ENTITY_ID_2");
    entityToRelate.setDomainId("TEST_DOMAIN");
    TimelineEntity entityToStore = new TimelineEntity();
    entityToStore.setEntityType("TEST_ENTITY_TYPE_1");
    entityToStore.setEntityId("TEST_ENTITY_ID_1");
    entityToStore.setDomainId("TEST_DOMAIN");
    entityToStore.addRelatedEntity("TEST_ENTITY_TYPE_2", "TEST_ENTITY_ID_2");
    TimelineEntities entities = new TimelineEntities();
    entities.addEntity(entityToStore);
    entities.addEntity(entityToRelate);
    store.put(entities);
    TimelineEntity entityToGet =
        store.getEntity("TEST_ENTITY_ID_2", "TEST_ENTITY_TYPE_2", null);
    Assert.assertNotNull(entityToGet);
    Assert.assertEquals("TEST_DOMAIN", entityToGet.getDomainId());
    Assert.assertEquals("TEST_ENTITY_TYPE_1",
        entityToGet.getRelatedEntities().keySet().iterator().next());
    Assert.assertEquals("TEST_ENTITY_ID_1",
        entityToGet.getRelatedEntities().values().iterator().next()
            .iterator().next());
  }

  @Test
  public void testRelatingToOldEntityWithoutDomainId() throws IOException {
    TimelineEntity entityToStore = new TimelineEntity();
    entityToStore.setEntityType("NEW_ENTITY_TYPE_1");
    entityToStore.setEntityId("NEW_ENTITY_ID_1");
    entityToStore.setDomainId(TimelineDataManager.DEFAULT_DOMAIN_ID);
    entityToStore.addRelatedEntity("OLD_ENTITY_TYPE_1", "OLD_ENTITY_ID_1");
    TimelineEntities entities = new TimelineEntities();
    entities.addEntity(entityToStore);
    store.put(entities);

    TimelineEntity entityToGet =
        store.getEntity("OLD_ENTITY_ID_1", "OLD_ENTITY_TYPE_1", null);
    Assert.assertNotNull(entityToGet);
    Assert.assertEquals("DEFAULT", entityToGet.getDomainId());
    Assert.assertEquals("NEW_ENTITY_TYPE_1",
        entityToGet.getRelatedEntities().keySet().iterator().next());
    Assert.assertEquals("NEW_ENTITY_ID_1",
        entityToGet.getRelatedEntities().values().iterator().next()
            .iterator().next());

    entityToStore = new TimelineEntity();
    entityToStore.setEntityType("NEW_ENTITY_TYPE_2");
    entityToStore.setEntityId("NEW_ENTITY_ID_2");
    entityToStore.setDomainId("NON_DEFAULT");
    entityToStore.addRelatedEntity("OLD_ENTITY_TYPE_1", "OLD_ENTITY_ID_1");
    entities = new TimelineEntities();
    entities.addEntity(entityToStore);
    TimelinePutResponse response = store.put(entities);
    Assert.assertEquals(1, response.getErrors().size());
    Assert.assertEquals(TimelinePutError.FORBIDDEN_RELATION,
        response.getErrors().get(0).getErrorCode());
    entityToGet =
        store.getEntity("OLD_ENTITY_ID_1", "OLD_ENTITY_TYPE_1", null);
    Assert.assertNotNull(entityToGet);
    Assert.assertEquals("DEFAULT", entityToGet.getDomainId());
    Assert.assertEquals(1, entityToGet.getRelatedEntities().keySet().size());
    Assert.assertEquals(1, entityToGet.getRelatedEntities().values()
        .iterator().next().size());
  }

  public void testStorePerformance() throws IOException {
    TimelineEntity entityToStorePrep = new TimelineEntity();
    entityToStorePrep.setEntityType("TEST_ENTITY_TYPE_PREP");
    entityToStorePrep.setEntityId("TEST_ENTITY_ID_PREP");
    entityToStorePrep.setDomainId("TEST_DOMAIN");
    entityToStorePrep.addRelatedEntity("TEST_ENTITY_TYPE_2",
        "TEST_ENTITY_ID_2");
    entityToStorePrep.setStartTime(0L);

    TimelineEntities entitiesPrep = new TimelineEntities();
    entitiesPrep.addEntity(entityToStorePrep);
    store.put(entitiesPrep);

    long start = System.currentTimeMillis();
    int num = 1000000;

    Log.info("Start test for " + num);

    final String tezTaskAttemptId = "TEZ_TA";
    final String tezEntityId = "attempt_1429158534256_0001_1_00_000000_";
    final String tezTaskId = "TEZ_T";
    final String tezDomainId = "Tez_ATS_application_1429158534256_0001";

    TimelineEntity entityToStore = new TimelineEntity();
    TimelineEvent startEvt = new TimelineEvent();
    entityToStore.setEntityType(tezTaskAttemptId);

    startEvt.setEventType("TASK_ATTEMPT_STARTED");
    startEvt.setTimestamp(0);
    entityToStore.addEvent(startEvt);
    entityToStore.setDomainId(tezDomainId);

    entityToStore.addPrimaryFilter("status", "SUCCEEDED");
    entityToStore.addPrimaryFilter("applicationId",
        "application_1429158534256_0001");
    entityToStore.addPrimaryFilter("TEZ_VERTEX_ID",
        "vertex_1429158534256_0001_1_00");
    entityToStore.addPrimaryFilter("TEZ_DAG_ID", "dag_1429158534256_0001_1");
    entityToStore.addPrimaryFilter("TEZ_TASK_ID",
        "task_1429158534256_0001_1_00_000000");

    entityToStore.setStartTime(0L);
    entityToStore.addOtherInfo("startTime", 0);
    entityToStore.addOtherInfo("inProgressLogsURL",
        "localhost:8042/inProgressLogsURL");
    entityToStore.addOtherInfo("completedLogsURL", "");
    entityToStore.addOtherInfo("nodeId", "localhost:54450");
    entityToStore.addOtherInfo("nodeHttpAddress", "localhost:8042");
    entityToStore.addOtherInfo("containerId",
        "container_1429158534256_0001_01_000002");
    entityToStore.addOtherInfo("status", "RUNNING");
    entityToStore.addRelatedEntity(tezTaskId, "TEZ_TASK_ID_1");

    TimelineEntities entities = new TimelineEntities();
    entities.addEntity(entityToStore);

    for (int i = 0; i < num; ++i) {
      entityToStore.setEntityId(tezEntityId + i);
      store.put(entities);
    }

    long duration = System.currentTimeMillis() - start;
    Log.info("Duration for " + num + ": " + duration);
  }

  public static void main(String[] args) throws Exception {
    TestRollingLevelDBTimelineStore store =
        new TestRollingLevelDBTimelineStore();
    store.setup();
    store.testStorePerformance();
    store.tearDown();
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-applicationhistoryservice/src/test/java/org/apache/hadoop/yarn/server/timeline/TimelineStoreTestUtils.java
    Set<Object> l1 = new HashSet<Object>();
    l1.add("username");
    Set<Object> l2 = new HashSet<Object>();
    l2.add(Integer.MAX_VALUE);
    Set<Object> l3 = new HashSet<Object>();
    l3.add("123abc");
    Set<Object> l4 = new HashSet<Object>();
    primaryFilters.put("other", l3);
    primaryFilters.put("long", l4);
    Map<String, Object> secondaryFilters = new HashMap<String, Object>();
    secondaryFilters.put("startTime", 123456);
    secondaryFilters.put("status", "RUNNING");
    Map<String, Object> otherInfo1 = new HashMap<String, Object>();
    otherInfo1.put("info1", "val1");
    relatedEntities.put(entityType2, Collections.singleton(entityId2));

    TimelineEvent ev3 = createEvent(789l, "launch_event", null);
    TimelineEvent ev4 = createEvent(0l, "init_event", null);
    List<TimelineEvent> events = new ArrayList<TimelineEvent>();
    events.add(ev3);
    events.add(ev4);
    relEntityMap2.put(entityType4, Collections.singleton(entityId4));

    ev3 = createEvent(789l, "launch_event", null);
    ev4 = createEvent(0l, "init_event", null);
    events2 = new ArrayList<TimelineEvent>();
    events2.add(ev3);
    events2.add(ev4);
        entityType1, EnumSet.allOf(Field.class)), domainId1);

    verifyEntityInfo(entityId2, entityType2, events2, relEntityMap,
        EMPTY_PRIMARY_FILTERS, EMPTY_MAP, 0l, store.getEntity(entityId2,
        entityType2, EnumSet.allOf(Field.class)), domainId1);

    verifyEntityInfo(entityId4, entityType4, EMPTY_EVENTS, EMPTY_REL_ENTITIES,

