hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-hs/src/main/java/org/apache/hadoop/mapreduce/v2/hs/webapp/HsController.java

package org.apache.hadoop.mapreduce.v2.hs.webapp;

import static org.apache.hadoop.yarn.webapp.YarnWebParams.ENTITY_STRING;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.v2.app.webapp.App;
import org.apache.hadoop.mapreduce.v2.app.webapp.AppController;
import org.apache.hadoop.yarn.webapp.View;
  public void logs() {
    String logEntity = $(ENTITY_STRING);
    JobID jid = null;
    try {
      jid = JobID.forName(logEntity);
      set(JOB_ID, logEntity);
      requireJob();
    } catch (Exception e) {
    }

    if (jid == null) {
      try {
        TaskAttemptID taskAttemptId = TaskAttemptID.forName(logEntity);
        set(TASK_ID, taskAttemptId.getTaskID().toString());
        set(JOB_ID, taskAttemptId.getJobID().toString());
        requireTask();
        requireJob();
      } catch (Exception e) {
      }
    }
    render(HsLogsPage.class);
  }


hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-hs/src/main/java/org/apache/hadoop/mapreduce/v2/hs/webapp/HsCountersPage.java

package org.apache.hadoop.mapreduce.v2.hs.webapp;

import static org.apache.hadoop.yarn.webapp.view.JQueryUI.*;

import org.apache.hadoop.mapreduce.v2.app.webapp.CountersBlock;
  @Override protected void preHead(Page.HTML<_> html) {
    commonPreHead(html);
    setActiveNavColumnForTask();
    set(DATATABLES_SELECTOR, "#counters .dt-counters");
    set(initSelector(DATATABLES),
        "{bJQueryUI:true, sDom:'t', iDisplayLength:-1}");

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-hs/src/main/java/org/apache/hadoop/mapreduce/v2/hs/webapp/HsLogsPage.java
package org.apache.hadoop.mapreduce.v2.hs.webapp;

import org.apache.hadoop.yarn.webapp.SubView;
import org.apache.hadoop.yarn.webapp.log.AggregatedLogsBlock;

  @Override protected void preHead(Page.HTML<_> html) {
    commonPreHead(html);
    setActiveNavColumnForTask();
  }


hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-hs/src/main/java/org/apache/hadoop/mapreduce/v2/hs/webapp/HsSingleCounterPage.java

package org.apache.hadoop.mapreduce.v2.hs.webapp;

import static org.apache.hadoop.yarn.webapp.view.JQueryUI.*;

import org.apache.hadoop.mapreduce.v2.app.webapp.SingleCounterBlock;
  @Override protected void preHead(Page.HTML<_> html) {
    commonPreHead(html);
    setActiveNavColumnForTask();
    set(DATATABLES_ID, "singleCounter");
    set(initID(DATATABLES, "singleCounter"), counterTableInit());
    setTableStyles(html, "singleCounter");

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-hs/src/main/java/org/apache/hadoop/mapreduce/v2/hs/webapp/HsView.java

package org.apache.hadoop.mapreduce.v2.hs.webapp;

import static org.apache.hadoop.mapreduce.v2.app.webapp.AMParams.TASK_ID;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.ACCORDION;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.ACCORDION_ID;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.DATATABLES;
    set(initID(ACCORDION, "nav"), "{autoHeight:false, active:0}");
  }

  protected void setActiveNavColumnForTask() {
    String tid = $(TASK_ID);
    String activeNav = "2";
    if((tid == null || tid.isEmpty())) {
      activeNav = "1";
    }
    set(initID(ACCORDION, "nav"), "{autoHeight:false, active:"+activeNav+"}");
  }


