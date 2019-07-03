hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/webapp/TestRMWebServicesApps.java
           WebServicesTestUtils.getXmlString(element, "name"),
           WebServicesTestUtils.getXmlString(element, "applicationType"),
           WebServicesTestUtils.getXmlString(element, "queue"),
           WebServicesTestUtils.getXmlInt(element, "priority"),
           WebServicesTestUtils.getXmlString(element, "state"),
           WebServicesTestUtils.getXmlString(element, "finalStatus"),
           WebServicesTestUtils.getXmlFloat(element, "progress"),
   public void verifyAppInfo(JSONObject info, RMApp app) throws JSONException,
       Exception {
 
     assertEquals("incorrect number of elements", 30, info.length());
 
     verifyAppInfoGeneric(app, info.getString("id"), info.getString("user"),
         info.getString("name"), info.getString("applicationType"),
         info.getString("queue"), info.getInt("priority"),
         info.getString("state"), info.getString("finalStatus"),
         (float) info.getDouble("progress"), info.getString("trackingUI"),
         info.getString("diagnostics"), info.getLong("clusterId"),
         info.getLong("startedTime"), info.getLong("finishedTime"),
         info.getLong("elapsedTime"), info.getString("amHostHttpAddress"),
         info.getString("amContainerLogs"), info.getInt("allocatedMB"),
         info.getInt("allocatedVCores"), info.getInt("runningContainers"),
         info.getInt("preemptedResourceMB"),
         info.getInt("preemptedResourceVCores"),
         info.getInt("numNonAMContainerPreempted"),
   }
 
   public void verifyAppInfoGeneric(RMApp app, String id, String user,
       String name, String applicationType, String queue, int prioirty,
       String state, String finalStatus, float progress, String trackingUI,
       String diagnostics, long clusterId, long startedTime, long finishedTime,
       long elapsedTime, String amHostHttpAddress, String amContainerLogs,
       int allocatedMB, int allocatedVCores, int numContainers,
     WebServicesTestUtils.checkStringMatch("applicationType",
       app.getApplicationType(), applicationType);
     WebServicesTestUtils.checkStringMatch("queue", app.getQueue(), queue);
     assertEquals("priority doesn't match", 0, prioirty);
     WebServicesTestUtils.checkStringMatch("state", app.getState().toString(),
         state);
     WebServicesTestUtils.checkStringMatch("finalStatus", app

