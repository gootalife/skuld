hadoop-common-project/hadoop-common/src/test/java/org/apache/hadoop/security/authorize/TestAccessControlList.java
    List<String> jerryLeeLewisGroups = groups.getGroups("jerryLeeLewis");
    assertTrue(jerryLeeLewisGroups.contains("@memphis"));

    UserGroupInformation elvis = 
      UserGroupInformation.createRemoteUser("elvis");
    assertUserAllowed(elvis, acl);

