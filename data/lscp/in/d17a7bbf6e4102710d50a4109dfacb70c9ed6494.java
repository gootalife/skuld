hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/test/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/linux/privileged/TestPrivilegedOperationExecutor.java
          .squashCGroupOperations(ops);
      String expected = new StringBuffer
          (PrivilegedOperation.CGROUP_ARG_PREFIX)
          .append(cGroupTasks1).append(PrivilegedOperation
              .LINUX_FILE_PATH_SEPARATOR)
          .append(cGroupTasks2).append(PrivilegedOperation
              .LINUX_FILE_PATH_SEPARATOR)
          .append(cGroupTasks3).toString();


