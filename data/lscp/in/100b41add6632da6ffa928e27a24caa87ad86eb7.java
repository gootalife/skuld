hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/TestContainerAllocation.java
    }

    SecurityUtilTestHelper.setTokenServiceUseIp(false);
    MockRM.launchAndRegisterAM(app1, rm1, nm1);
  }
}

