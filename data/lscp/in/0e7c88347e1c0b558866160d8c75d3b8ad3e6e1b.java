hadoop-yarn-project/hadoop-yarn/hadoop-yarn-api/src/test/java/org/apache/hadoop/yarn/conf/TestYarnConfigurationFields.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-api/src/test/java/org/apache/hadoop/yarn/conf/TestYarnConfigurationFields.java

package org.apache.hadoop.yarn.conf;

import java.util.HashSet;

import org.apache.hadoop.conf.TestConfigurationFieldsBase;

public class TestYarnConfigurationFields extends TestConfigurationFieldsBase {

  @SuppressWarnings("deprecation")
  @Override
  public void initializeMemberVariables() {
    xmlFilename = new String("yarn-default.xml");
    configurationClasses = new Class[] { YarnConfiguration.class };

    configurationPropsToSkipCompare = new HashSet<String>();
    configurationPrefixToSkipCompare = new HashSet<String>();

    errorIfMissingConfigProps = true;
    errorIfMissingXmlProps = true;

    configurationPropsToSkipCompare
        .add(YarnConfiguration.DEFAULT_RM_CONFIGURATION_PROVIDER_CLASS);
    configurationPropsToSkipCompare
        .add(YarnConfiguration.DEFAULT_CLIENT_FAILOVER_PROXY_PROVIDER);
    configurationPropsToSkipCompare
        .add(YarnConfiguration.DEFAULT_IPC_RECORD_FACTORY_CLASS);
    configurationPropsToSkipCompare
        .add(YarnConfiguration.DEFAULT_IPC_CLIENT_FACTORY_CLASS);
    configurationPropsToSkipCompare
        .add(YarnConfiguration.DEFAULT_IPC_SERVER_FACTORY_CLASS);
    configurationPropsToSkipCompare
        .add(YarnConfiguration.DEFAULT_IPC_RPC_IMPL);
    configurationPropsToSkipCompare
        .add(YarnConfiguration.DEFAULT_RM_SCHEDULER);
    configurationPropsToSkipCompare
        .add(YarnConfiguration
            .YARN_SECURITY_SERVICE_AUTHORIZATION_APPLICATIONCLIENT_PROTOCOL);
    configurationPropsToSkipCompare
        .add(YarnConfiguration
            .YARN_SECURITY_SERVICE_AUTHORIZATION_APPLICATIONMASTER_PROTOCOL);
    configurationPropsToSkipCompare
        .add(YarnConfiguration
            .YARN_SECURITY_SERVICE_AUTHORIZATION_CONTAINER_MANAGEMENT_PROTOCOL);
    configurationPropsToSkipCompare
        .add(YarnConfiguration
            .YARN_SECURITY_SERVICE_AUTHORIZATION_RESOURCE_LOCALIZER);
    configurationPropsToSkipCompare
        .add(YarnConfiguration
            .YARN_SECURITY_SERVICE_AUTHORIZATION_RESOURCEMANAGER_ADMINISTRATION_PROTOCOL);
    configurationPropsToSkipCompare
        .add(YarnConfiguration
            .YARN_SECURITY_SERVICE_AUTHORIZATION_RESOURCETRACKER_PROTOCOL);
    configurationPropsToSkipCompare
        .add(YarnConfiguration.DEFAULT_SCM_STORE_CLASS);
    configurationPropsToSkipCompare
        .add(YarnConfiguration.DEFAULT_SCM_APP_CHECKER_CLASS);
    configurationPropsToSkipCompare
        .add(YarnConfiguration.DEFAULT_SHARED_CACHE_CHECKSUM_ALGO_IMPL);

    configurationPrefixToSkipCompare.add("yarn.timeline-service.");

    configurationPrefixToSkipCompare.add("yarn.app.container");

    configurationPrefixToSkipCompare
        .add(YarnConfiguration.NM_NETWORK_RESOURCE_ENABLED);
    configurationPrefixToSkipCompare
        .add(YarnConfiguration.NM_NETWORK_RESOURCE_INTERFACE);
    configurationPrefixToSkipCompare
        .add(YarnConfiguration.NM_NETWORK_RESOURCE_OUTBOUND_BANDWIDTH_MBIT);
    configurationPrefixToSkipCompare
        .add(YarnConfiguration.NM_NETWORK_RESOURCE_OUTBOUND_BANDWIDTH_YARN_MBIT);
    configurationPrefixToSkipCompare
        .add(YarnConfiguration.NM_DISK_RESOURCE_ENABLED);

    configurationPrefixToSkipCompare.add(YarnConfiguration.NM_USER_HOME_DIR);

    configurationPrefixToSkipCompare
        .add(YarnConfiguration.YARN_CLIENT_APP_SUBMISSION_POLL_INTERVAL_MS);

    xmlPropsToSkipCompare = new HashSet<String>();
    xmlPrefixToSkipCompare = new HashSet<String>();

    xmlPropsToSkipCompare.add("mapreduce.job.hdfs-servers");
    xmlPropsToSkipCompare.add("mapreduce.job.jar");

    xmlPropsToSkipCompare.add("yarn.nodemanager.aux-services.mapreduce_shuffle.class");
    xmlPropsToSkipCompare.add("yarn.resourcemanager.container.liveness-monitor.interval-ms");

    xmlPropsToSkipCompare.add("yarn.nodemanager.hostname");

    xmlPrefixToSkipCompare.add("yarn.timeline-service");

    xmlPrefixToSkipCompare.add("hadoop.registry");
  }
}

