hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/webapp/JAXBContextResolver.java
            NodesInfo.class, RemoteExceptionData.class,
            CapacitySchedulerQueueInfoList.class, ResourceInfo.class,
            UsersInfo.class, UserInfo.class, ApplicationStatisticsInfo.class,
            StatisticsItemInfo.class, CapacitySchedulerHealthInfo.class,
            FairSchedulerQueueInfoList.class};
    final Class[] rootUnwrappedTypes =
        { NewApplication.class, ApplicationSubmissionContextInfo.class,

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/webapp/dao/FairSchedulerQueueInfo.java
package org.apache.hadoop.yarn.server.resourcemanager.webapp.dao;


import java.util.Collection;

import javax.xml.bind.annotation.XmlAccessType;
  private String queueName;
  private String schedulingPolicy;

  private FairSchedulerQueueInfoList childQueues;

  public FairSchedulerQueueInfo() {
  }
    
    maxApps = allocConf.getQueueMaxApps(queueName);

    if (allocConf.isReservable(queueName) &&
        !allocConf.getShowReservationAsQueues(queueName)) {
      return;
    }

    childQueues = getChildQueues(queue, scheduler);
  }

  protected FairSchedulerQueueInfoList getChildQueues(FSQueue queue,
                                                      FairScheduler scheduler) {
    Collection<FSQueue> children = queue.getChildQueues();
    if (children.isEmpty()) {
      return null;
    }
    FairSchedulerQueueInfoList list = new FairSchedulerQueueInfoList();
    for (FSQueue child : children) {
      if (child instanceof FSLeafQueue) {
        list.addToQueueInfoList(
            new FairSchedulerLeafQueueInfo((FSLeafQueue) child, scheduler));
      } else {
        list.addToQueueInfoList(
            new FairSchedulerQueueInfo(child, scheduler));
      }
    }
    return list;
  }
  
  }

  public Collection<FairSchedulerQueueInfo> getChildQueues() {
    return childQueues.getQueueInfoList();
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/webapp/dao/FairSchedulerQueueInfoList.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/webapp/dao/FairSchedulerQueueInfoList.java
package org.apache.hadoop.yarn.server.resourcemanager.webapp.dao;

import java.util.ArrayList;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class FairSchedulerQueueInfoList {
  private ArrayList<FairSchedulerQueueInfo> queue;

  public FairSchedulerQueueInfoList() {
    queue = new ArrayList<>();
  }

  public ArrayList<FairSchedulerQueueInfo> getQueueInfoList() {
    return this.queue;
  }

  public boolean addToQueueInfoList(FairSchedulerQueueInfo e) {
    return this.queue.add(e);
  }

  public FairSchedulerQueueInfo getQueueInfo(int i) {
    return this.queue.get(i);
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/webapp/TestRMWebServicesCapacitySched.java
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.StringReader;

        user.getInt("numPendingApplications");
        checkResourcesUsed(user);
      }

      try {
        b1.getJSONObject("queues");
        fail("CapacitySchedulerQueueInfo should omit field 'queues'" +
             "if child queue is empty.");
      } catch (JSONException je) {
        assertEquals("JSONObject[\"queues\"] not found.", je.getMessage());
      }
    } finally {
      rm.stop();
    }

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/webapp/TestRMWebServicesFairScheduler.java
package org.apache.hadoop.yarn.server.resourcemanager.webapp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import javax.ws.rs.core.MediaType;

import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.QueueManager;
import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
import org.apache.hadoop.yarn.webapp.JerseyTestBase;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.Test;
    verifyClusterScheduler(json);
  }
  
  @Test
  public void testClusterSchedulerWithSubQueues() throws JSONException,
      Exception {
    FairScheduler scheduler = (FairScheduler)rm.getResourceScheduler();
    QueueManager queueManager = scheduler.getQueueManager();
    queueManager.getLeafQueue("root.q.subqueue1", true);
    queueManager.getLeafQueue("root.q.subqueue2", true);

    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("cluster")
        .path("scheduler").accept(MediaType.APPLICATION_JSON)
        .get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject json = response.getEntity(JSONObject.class);
    JSONArray subQueueInfo = json.getJSONObject("scheduler")
        .getJSONObject("schedulerInfo").getJSONObject("rootQueue")
        .getJSONObject("childQueues").getJSONArray("queue")
        .getJSONObject(1).getJSONObject("childQueues").getJSONArray("queue");
    assertEquals(2, subQueueInfo.length());

    try {
      subQueueInfo.getJSONObject(1).getJSONObject("childQueues");
      fail("FairSchedulerQueueInfo should omit field 'childQueues'" +
           "if child queue is empty.");
    } catch (JSONException je) {
      assertEquals("JSONObject[\"childQueues\"] not found.", je.getMessage());
    }
  }

  private void verifyClusterScheduler(JSONObject json) throws JSONException,
      Exception {
    assertEquals("incorrect number of elements", 1, json.length());

