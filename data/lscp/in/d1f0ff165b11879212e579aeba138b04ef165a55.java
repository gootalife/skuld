hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-applicationhistoryservice/src/main/java/org/apache/hadoop/yarn/server/timeline/TimelineDataManager.java
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
  @VisibleForTesting
  public static final String DEFAULT_DOMAIN_ID = "DEFAULT";

  private TimelineDataManagerMetrics metrics;
  private TimelineStore store;
  private TimelineACLsManager timelineACLsManager;


  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    metrics = TimelineDataManagerMetrics.create();
    TimelineDomain domain = store.getDomain("DEFAULT");
      Long limit,
      EnumSet<Field> fields,
      UserGroupInformation callerUGI) throws YarnException, IOException {
    long startTime = Time.monotonicNow();
    metrics.incrGetEntitiesOps();
    try {
      TimelineEntities entities = doGetEntities(
          entityType,
          primaryFilter,
          secondaryFilter,
          windowStart,
          windowEnd,
          fromId,
          fromTs,
          limit,
          fields,
          callerUGI);
      metrics.incrGetEntitiesTotal(entities.getEntities().size());
      return entities;
    } finally {
      metrics.addGetEntitiesTime(Time.monotonicNow() - startTime);
    }
  }

  private TimelineEntities doGetEntities(
      String entityType,
      NameValuePair primaryFilter,
      Collection<NameValuePair> secondaryFilter,
      Long windowStart,
      Long windowEnd,
      String fromId,
      Long fromTs,
      Long limit,
      EnumSet<Field> fields,
      UserGroupInformation callerUGI) throws YarnException, IOException {
    TimelineEntities entities = null;
    entities = store.getEntities(
        entityType,
      String entityId,
      EnumSet<Field> fields,
      UserGroupInformation callerUGI) throws YarnException, IOException {
    long startTime = Time.monotonicNow();
    metrics.incrGetEntityOps();
    try {
      return doGetEntity(entityType, entityId, fields, callerUGI);
    } finally {
      metrics.addGetEntityTime(Time.monotonicNow() - startTime);
    }
  }

  private TimelineEntity doGetEntity(
      String entityType,
      String entityId,
      EnumSet<Field> fields,
      UserGroupInformation callerUGI) throws YarnException, IOException {
    TimelineEntity entity = null;
    entity =
        store.getEntity(entityId, entityType, fields);
      Long windowEnd,
      Long limit,
      UserGroupInformation callerUGI) throws YarnException, IOException {
    long startTime = Time.monotonicNow();
    metrics.incrGetEventsOps();
    try {
      TimelineEvents events = doGetEvents(
          entityType,
          entityIds,
          eventTypes,
          windowStart,
          windowEnd,
          limit,
          callerUGI);
      metrics.incrGetEventsTotal(events.getAllEvents().size());
      return events;
    } finally {
      metrics.addGetEventsTime(Time.monotonicNow() - startTime);
    }
  }

  private TimelineEvents doGetEvents(
      String entityType,
      SortedSet<String> entityIds,
      SortedSet<String> eventTypes,
      Long windowStart,
      Long windowEnd,
      Long limit,
      UserGroupInformation callerUGI) throws YarnException, IOException {
    TimelineEvents events = null;
    events = store.getEntityTimelines(
        entityType,
  public TimelinePutResponse postEntities(
      TimelineEntities entities,
      UserGroupInformation callerUGI) throws YarnException, IOException {
    long startTime = Time.monotonicNow();
    metrics.incrPostEntitiesOps();
    try {
      return doPostEntities(entities, callerUGI);
    } finally {
      metrics.addPostEntitiesTime(Time.monotonicNow() - startTime);
    }
  }

  private TimelinePutResponse doPostEntities(
      TimelineEntities entities,
      UserGroupInformation callerUGI) throws YarnException, IOException {
    if (entities == null) {
      return new TimelinePutResponse();
    }
    metrics.incrPostEntitiesTotal(entities.getEntities().size());
    TimelineEntities entitiesToPut = new TimelineEntities();
    List<TimelinePutResponse.TimelinePutError> errors =
        new ArrayList<TimelinePutResponse.TimelinePutError>();
  public void putDomain(TimelineDomain domain,
      UserGroupInformation callerUGI) throws YarnException, IOException {
    long startTime = Time.monotonicNow();
    metrics.incrPutDomainOps();
    try {
      doPutDomain(domain, callerUGI);
    } finally {
      metrics.addPutDomainTime(Time.monotonicNow() - startTime);
    }
  }

  private void doPutDomain(TimelineDomain domain,
      UserGroupInformation callerUGI) throws YarnException, IOException {
    TimelineDomain existingDomain =
        store.getDomain(domain.getId());
    if (existingDomain != null) {
  public TimelineDomain getDomain(String domainId,
      UserGroupInformation callerUGI) throws YarnException, IOException {
    long startTime = Time.monotonicNow();
    metrics.incrGetDomainOps();
    try {
      return doGetDomain(domainId, callerUGI);
    } finally {
      metrics.addGetDomainTime(Time.monotonicNow() - startTime);
    }
  }

  private TimelineDomain doGetDomain(String domainId,
      UserGroupInformation callerUGI) throws YarnException, IOException {
    TimelineDomain domain = store.getDomain(domainId);
    if (domain != null) {
      if (timelineACLsManager.checkAccess(callerUGI, domain)) {
  public TimelineDomains getDomains(String owner,
      UserGroupInformation callerUGI) throws YarnException, IOException {
    long startTime = Time.monotonicNow();
    metrics.incrGetDomainsOps();
    try {
      TimelineDomains domains = doGetDomains(owner, callerUGI);
      metrics.incrGetDomainsTotal(domains.getDomains().size());
      return domains;
    } finally {
      metrics.addGetDomainsTime(Time.monotonicNow() - startTime);
    }
  }

  private TimelineDomains doGetDomains(String owner,
      UserGroupInformation callerUGI) throws YarnException, IOException {
    TimelineDomains domains = store.getDomains(owner);
    boolean hasAccess = true;
    if (domains.getDomains().size() > 0) {

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-applicationhistoryservice/src/main/java/org/apache/hadoop/yarn/server/timeline/TimelineDataManagerMetrics.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-applicationhistoryservice/src/main/java/org/apache/hadoop/yarn/server/timeline/TimelineDataManagerMetrics.java
package org.apache.hadoop.yarn.server.timeline;

import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableRate;

@Metrics(about="Metrics for TimelineDataManager", context="yarn")
public class TimelineDataManagerMetrics {
  @Metric("getEntities calls")
  MutableCounterLong getEntitiesOps;

  @Metric("Entities returned via getEntities")
  MutableCounterLong getEntitiesTotal;

  @Metric("getEntities processing time")
  MutableRate getEntitiesTime;

  @Metric("getEntity calls")
  MutableCounterLong getEntityOps;

  @Metric("getEntity processing time")
  MutableRate getEntityTime;

  @Metric("getEvents calls")
  MutableCounterLong getEventsOps;

  @Metric("Events returned via getEvents")
  MutableCounterLong getEventsTotal;

  @Metric("getEvents processing time")
  MutableRate getEventsTime;

  @Metric("postEntities calls")
  MutableCounterLong postEntitiesOps;

  @Metric("Entities posted via postEntities")
  MutableCounterLong postEntitiesTotal;

  @Metric("postEntities processing time")
  MutableRate postEntitiesTime;

  @Metric("putDomain calls")
  MutableCounterLong putDomainOps;

  @Metric("putDomain processing time")
  MutableRate putDomainTime;

  @Metric("getDomain calls")
  MutableCounterLong getDomainOps;

  @Metric("getDomain processing time")
  MutableRate getDomainTime;

  @Metric("getDomains calls")
  MutableCounterLong getDomainsOps;

  @Metric("Domains returned via getDomains")
  MutableCounterLong getDomainsTotal;

  @Metric("getDomains processing time")
  MutableRate getDomainsTime;

  @Metric("Total calls")
  public long totalOps() {
    return getEntitiesOps.value() +
        getEntityOps.value() +
        getEventsOps.value() +
        postEntitiesOps.value() +
        putDomainOps.value() +
        getDomainOps.value() +
        getDomainsOps.value();
  }

  TimelineDataManagerMetrics() {
  }

  public static TimelineDataManagerMetrics create() {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    return ms.register(new TimelineDataManagerMetrics());
  }

  public void incrGetEntitiesOps() {
    getEntitiesOps.incr();
  }

  public void incrGetEntitiesTotal(long delta) {
    getEntitiesTotal.incr(delta);
  }

  public void addGetEntitiesTime(long msec) {
    getEntitiesTime.add(msec);
  }

  public void incrGetEntityOps() {
    getEntityOps.incr();
  }

  public void addGetEntityTime(long msec) {
    getEntityTime.add(msec);
  }

  public void incrGetEventsOps() {
    getEventsOps.incr();
  }

  public void incrGetEventsTotal(long delta) {
    getEventsTotal.incr(delta);
  }

  public void addGetEventsTime(long msec) {
    getEventsTime.add(msec);
  }

  public void incrPostEntitiesOps() {
    postEntitiesOps.incr();
  }

  public void incrPostEntitiesTotal(long delta) {
    postEntitiesTotal.incr(delta);
  }

  public void addPostEntitiesTime(long msec) {
    postEntitiesTime.add(msec);
  }

  public void incrPutDomainOps() {
    putDomainOps.incr();
  }

  public void addPutDomainTime(long msec) {
    putDomainTime.add(msec);
  }

  public void incrGetDomainOps() {
    getDomainOps.incr();
  }

  public void addGetDomainTime(long msec) {
    getDomainTime.add(msec);
  }

  public void incrGetDomainsOps() {
    getDomainsOps.incr();
  }

  public void incrGetDomainsTotal(long delta) {
    getDomainsTotal.incr(delta);
  }

  public void addGetDomainsTime(long msec) {
    getDomainsTime.add(msec);
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-applicationhistoryservice/src/test/java/org/apache/hadoop/yarn/server/applicationhistoryservice/TestApplicationHistoryClientService.java
    TimelineACLsManager aclsManager = new TimelineACLsManager(conf);
    dataManager =
        new TimelineDataManager(store, aclsManager);
    dataManager.init(conf);
    ApplicationACLsManager appAclsManager = new ApplicationACLsManager(conf);
    ApplicationHistoryManagerOnTimelineStore historyManager =
        new ApplicationHistoryManagerOnTimelineStore(dataManager, appAclsManager);

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-applicationhistoryservice/src/test/java/org/apache/hadoop/yarn/server/applicationhistoryservice/TestApplicationHistoryManagerOnTimelineStore.java
    TimelineACLsManager aclsManager = new TimelineACLsManager(new YarnConfiguration());
    TimelineDataManager dataManager =
        new TimelineDataManager(store, aclsManager);
    dataManager.init(conf);
    ApplicationACLsManager appAclsManager = new ApplicationACLsManager(conf);
    historyManager =
        new ApplicationHistoryManagerOnTimelineStore(dataManager, appAclsManager);

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-applicationhistoryservice/src/test/java/org/apache/hadoop/yarn/server/applicationhistoryservice/webapp/TestAHSWebServices.java
        new TimelineDataManager(store, aclsManager);
    conf.setBoolean(YarnConfiguration.YARN_ACL_ENABLE, true);
    conf.set(YarnConfiguration.YARN_ADMIN_ACL, "foo");
    dataManager.init(conf);
    ApplicationACLsManager appAclsManager = new ApplicationACLsManager(conf);
    ApplicationHistoryManagerOnTimelineStore historyManager =
        new ApplicationHistoryManagerOnTimelineStore(dataManager, appAclsManager);

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-applicationhistoryservice/src/test/java/org/apache/hadoop/yarn/server/timeline/TestTimelineDataManager.java
    dataManaer = new TimelineDataManager(store, aclsManager);
    conf.setBoolean(YarnConfiguration.YARN_ACL_ENABLE, true);
    conf.set(YarnConfiguration.YARN_ADMIN_ACL, "admin");
    dataManaer.init(conf);
    adminACLsManager = new AdminACLsManager(conf);
  }


