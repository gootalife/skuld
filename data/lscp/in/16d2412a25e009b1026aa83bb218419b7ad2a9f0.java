hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/AdminService.java
  private final RecordFactory recordFactory = 
    RecordFactoryProvider.getRecordFactory(null);

  private UserGroupInformation daemonUser;

  @VisibleForTesting
  boolean isDistributedNodeLabelConfiguration = false;

        YarnConfiguration.RM_ADMIN_ADDRESS,
        YarnConfiguration.DEFAULT_RM_ADMIN_ADDRESS,
        YarnConfiguration.DEFAULT_RM_ADMIN_PORT);
    daemonUser = UserGroupInformation.getCurrentUser();
    authorizer = YarnAuthorizationProvider.getInstance(conf);
    authorizer.setAdmins(getAdminAclList(conf), UserGroupInformation
        .getCurrentUser());
    rmId = conf.get(YarnConfiguration.RM_HA_ID);

    super.serviceInit(conf);
  }

  private AccessControlList getAdminAclList(Configuration conf) {
    AccessControlList aclList = new AccessControlList(conf.get(
        YarnConfiguration.YARN_ADMIN_ACL,
        YarnConfiguration.DEFAULT_YARN_ADMIN_ACL));
    aclList.addUser(daemonUser.getShortUserName());
    return aclList;
  }

  @Override
  protected void serviceStart() throws Exception {
    startServer();
    Configuration conf =
        getConfiguration(new Configuration(false),
            YarnConfiguration.YARN_SITE_CONFIGURATION_FILE);
    authorizer.setAdmins(getAdminAclList(conf), UserGroupInformation
        .getCurrentUser());
    RMAuditLogger.logSuccess(user.getShortUserName(), argName,
        "AdminService");

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/TestRMAdminService.java
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.ha.HAServiceProtocol.StateChangeRequestInfo;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.GroupMappingServiceProvider;
import org.apache.hadoop.security.Groups;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.hadoop.security.authorize.ServiceAuthorizationManager;
import org.apache.hadoop.yarn.LocalConfigurationProvider;
import org.apache.hadoop.yarn.api.records.DecommissionType;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.conf.HAUtil;
        rm.adminService.getAccessControlList().getAclString().trim();

    Assert.assertTrue(!aclStringAfter.equals(aclStringBefore));
    Assert.assertEquals(aclStringAfter, "world:anyone:rwcda," +
        UserGroupInformation.getCurrentUser().getShortUserName());
  }

  @Test
      String aclStringAfter =
          resourceManager.adminService.getAccessControlList()
              .getAclString().trim();
      Assert.assertEquals(aclStringAfter, "world:anyone:rwcda," +
          UserGroupInformation.getCurrentUser().getShortUserName());

      CapacityScheduler cs =
    }
  }

  @Test
  public void testRefreshAclWithDaemonUser() throws Exception {
    String daemonUser =
        UserGroupInformation.getCurrentUser().getShortUserName();
    configuration.set(YarnConfiguration.RM_CONFIGURATION_PROVIDER_CLASS,
        "org.apache.hadoop.yarn.FileSystemBasedConfigurationProvider");

    uploadDefaultConfiguration();
    YarnConfiguration yarnConf = new YarnConfiguration();
    yarnConf.set(YarnConfiguration.YARN_ADMIN_ACL, daemonUser + "xyz");
    uploadConfiguration(yarnConf, "yarn-site.xml");

    try {
      rm = new MockRM(configuration);
      rm.init(configuration);
      rm.start();
    } catch(Exception ex) {
      fail("Should not get any exceptions");
    }

    assertEquals(daemonUser + "xyz," + daemonUser,
        rm.adminService.getAccessControlList().getAclString().trim());

    yarnConf = new YarnConfiguration();
    yarnConf.set(YarnConfiguration.YARN_ADMIN_ACL, daemonUser + "abc");
    uploadConfiguration(yarnConf, "yarn-site.xml");
    try {
      rm.adminService.refreshAdminAcls(RefreshAdminAclsRequest.newInstance());
    } catch (YarnException e) {
      if (e.getCause() != null &&
          e.getCause() instanceof AccessControlException) {
        fail("Refresh should not have failed due to incorrect ACL");
      }
      throw e;
    }

    assertEquals(daemonUser + "abc," + daemonUser,
        rm.adminService.getAccessControlList().getAclString().trim());
  }

  @Test
  public void testModifyLabelsOnNodesWithDistributedConfigurationDisabled()
      throws IOException, YarnException {

