hadoop-yarn-project/hadoop-yarn/hadoop-yarn-api/src/main/java/org/apache/hadoop/yarn/api/records/timeline/TimelineDelegationTokenResponse.java
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;

@XmlRootElement(name = "delegationtoken")
@XmlAccessorType(XmlAccessType.NONE)
@Public
@Evolving
public class TimelineDelegationTokenResponse {

  private String type;

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-api/src/main/java/org/apache/hadoop/yarn/api/records/timeline/TimelineDomain.java
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;

@XmlRootElement(name = "domain")
@XmlAccessorType(XmlAccessType.NONE)
@Public
@Evolving
public class TimelineDomain {

  private String id;

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-api/src/main/java/org/apache/hadoop/yarn/api/records/timeline/TimelineDomains.java
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;

@XmlRootElement(name = "domains")
@XmlAccessorType(XmlAccessType.NONE)
@Public
@Evolving
public class TimelineDomains {

  private List<TimelineDomain> domains = new ArrayList<TimelineDomain>();

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-api/src/main/java/org/apache/hadoop/yarn/api/records/timeline/TimelineEntities.java
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;

@XmlRootElement(name = "entities")
@XmlAccessorType(XmlAccessType.NONE)
@Public
@Evolving
public class TimelineEntities {

  private List<TimelineEntity> entities =

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-api/src/main/java/org/apache/hadoop/yarn/api/records/timeline/TimelineEntity.java

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;

@XmlRootElement(name = "entity")
@XmlAccessorType(XmlAccessType.NONE)
@Public
@Evolving
public class TimelineEntity implements Comparable<TimelineEntity> {

  private String entityType;

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-api/src/main/java/org/apache/hadoop/yarn/api/records/timeline/TimelineEvent.java

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;

@XmlRootElement(name = "event")
@XmlAccessorType(XmlAccessType.NONE)
@Public
@Evolving
public class TimelineEvent implements Comparable<TimelineEvent> {

  private long timestamp;

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-api/src/main/java/org/apache/hadoop/yarn/api/records/timeline/TimelineEvents.java
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;

@XmlRootElement(name = "events")
@XmlAccessorType(XmlAccessType.NONE)
@Public
@Evolving
public class TimelineEvents {

  private List<EventsOfOneEntity> allEvents =
  @XmlRootElement(name = "events")
  @XmlAccessorType(XmlAccessType.NONE)
  @Public
  @Evolving
  public static class EventsOfOneEntity {

    private String entityId;

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-api/src/main/java/org/apache/hadoop/yarn/api/records/timeline/TimelinePutResponse.java
package org.apache.hadoop.yarn.api.records.timeline;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
@XmlRootElement(name = "response")
@XmlAccessorType(XmlAccessType.NONE)
@Public
@Evolving
public class TimelinePutResponse {

  private List<TimelinePutError> errors = new ArrayList<TimelinePutError>();
  @XmlRootElement(name = "error")
  @XmlAccessorType(XmlAccessType.NONE)
  @Public
  @Evolving
  public static class TimelinePutError {


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-api/src/main/java/org/apache/hadoop/yarn/api/records/timeline/package-info.java
@InterfaceAudience.Public
@InterfaceStability.Evolving
package org.apache.hadoop.yarn.api.records.timeline;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/main/java/org/apache/hadoop/yarn/client/api/TimelineClient.java

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.service.AbstractService;
@Public
@Evolving
public abstract class TimelineClient extends AbstractService {


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/main/java/org/apache/hadoop/yarn/client/api/impl/TimelineClientImpl.java
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.SecurityUtil;
import com.sun.jersey.client.urlconnection.URLConnectionClientHandler;

@Private
@Evolving
public class TimelineClientImpl extends TimelineClient {

  private static final Log LOG = LogFactory.getLog(TimelineClientImpl.class);

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/main/java/org/apache/hadoop/yarn/client/api/impl/package-info.java
@InterfaceAudience.Public
@InterfaceStability.Evolving
package org.apache.hadoop.yarn.client.api.impl;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/main/java/org/apache/hadoop/yarn/client/api/package-info.java
@InterfaceAudience.Public
@InterfaceStability.Evolving
package org.apache.hadoop.yarn.client.api;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/main/java/org/apache/hadoop/yarn/security/client/TimelineDelegationTokenIdentifier.java

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.exceptions.YarnException;

@Public
@Evolving
public class TimelineDelegationTokenIdentifier extends YARNDelegationTokenIdentifier {

  public static final Text KIND_NAME = new Text("TIMELINE_DELEGATION_TOKEN");

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/main/java/org/apache/hadoop/yarn/security/client/TimelineDelegationTokenSelector.java
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.token.TokenSelector;

@Public
@Evolving
public class TimelineDelegationTokenSelector
    implements TokenSelector<TimelineDelegationTokenIdentifier> {


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-common/src/main/java/org/apache/hadoop/yarn/server/webapp/dao/AppAttemptInfo.java
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationAttemptState;

@Public
@Evolving
@XmlRootElement(name = "appAttempt")
@XmlAccessorType(XmlAccessType.FIELD)
public class AppAttemptInfo {

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-common/src/main/java/org/apache/hadoop/yarn/server/webapp/dao/AppAttemptsInfo.java
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;

@Public
@Evolving
@XmlRootElement(name = "appAttempts")
@XmlAccessorType(XmlAccessType.FIELD)
public class AppAttemptsInfo {

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-common/src/main/java/org/apache/hadoop/yarn/server/webapp/dao/AppInfo.java
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;

import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.util.Times;

@Public
@Evolving
@XmlRootElement(name = "app")
@XmlAccessorType(XmlAccessType.FIELD)
public class AppInfo {

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-common/src/main/java/org/apache/hadoop/yarn/server/webapp/dao/AppsInfo.java
package org.apache.hadoop.yarn.server.webapp.dao;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;

import java.util.ArrayList;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

@Public
@Evolving
@XmlRootElement(name = "apps")
@XmlAccessorType(XmlAccessType.FIELD)
public class AppsInfo {

  protected ArrayList<AppInfo> app = new ArrayList<>();

  public AppsInfo() {

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-common/src/main/java/org/apache/hadoop/yarn/server/webapp/dao/ContainerInfo.java
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;

import org.apache.hadoop.yarn.api.records.ContainerReport;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.util.Times;

@Public
@Evolving
@XmlRootElement(name = "container")
@XmlAccessorType(XmlAccessType.FIELD)
public class ContainerInfo {

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-common/src/main/java/org/apache/hadoop/yarn/server/webapp/dao/ContainersInfo.java
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;

@Public
@Evolving
@XmlRootElement(name = "containers")
@XmlAccessorType(XmlAccessType.FIELD)
public class ContainersInfo {

