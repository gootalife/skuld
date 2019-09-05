hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-app/src/main/java/org/apache/hadoop/mapreduce/jobhistory/JobHistoryEventHandler.java
        tEvent.addEventInfo("JOB_CONF_PATH", jse.getJobConfPath());
        tEvent.addEventInfo("ACLS", jse.getJobAcls());
        tEvent.addEventInfo("JOB_QUEUE_NAME", jse.getJobQueueName());
        tEvent.addEventInfo("WORKFLOW_ID", jse.getWorkflowId());
        tEvent.addEventInfo("WORKLFOW_ID", jse.getWorkflowId());
        tEvent.addEventInfo("WORKFLOW_NAME", jse.getWorkflowName());
        tEvent.addEventInfo("WORKFLOW_NAME_NAME", jse.getWorkflowNodeName());

