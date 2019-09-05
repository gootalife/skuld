hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-common/src/main/java/org/apache/hadoop/yarn/server/webapp/AppBlock.java
import org.apache.hadoop.yarn.api.records.ApplicationAttemptReport;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ContainerReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LogAggregationStatus;
      AppAttemptInfo appAttempt = new AppAttemptInfo(appAttemptReport);
      ContainerReport containerReport;
      try {
        final GetContainerReportRequest request =
                GetContainerReportRequest.newInstance(
                      appAttemptReport.getAMContainerId());
        if (callerUGI == null) {
          containerReport =
              appBaseProt.getContainerReport(request).getContainerReport();

