hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/ResourceManager.java
        this.configurationProvider.getConfigurationInputStream(this.conf,
            YarnConfiguration.CORE_SITE_CONFIGURATION_FILE);
    if (coreSiteXMLInputStream != null) {
      this.conf.addResource(coreSiteXMLInputStream,
          YarnConfiguration.CORE_SITE_CONFIGURATION_FILE);
    }

        this.configurationProvider.getConfigurationInputStream(this.conf,
            YarnConfiguration.YARN_SITE_CONFIGURATION_FILE);
    if (yarnSiteXMLInputStream != null) {
      this.conf.addResource(yarnSiteXMLInputStream,
          YarnConfiguration.YARN_SITE_CONFIGURATION_FILE);
    }

    validateConfigs(this.conf);

