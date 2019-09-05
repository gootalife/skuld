hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/AdminService.java
    rmContext.getResourceTrackerService().refreshServiceAcls(
        conf, policyProvider);

    RMAuditLogger.logSuccess(user.getShortUserName(), argName, "AdminService");

    return recordFactory.newRecordInstance(RefreshServiceAclsResponse.class);
  }


