hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-applicationhistoryservice/src/main/java/org/apache/hadoop/yarn/server/applicationhistoryservice/webapp/AHSView.java
  protected void preHead(Page.HTML<_> html) {
    commonPreHead(html);
    set(DATATABLES_ID, "apps");
    set(initID(DATATABLES, "apps"), WebPageUtils.appsTableInit(false));
    setTableStyles(html, "apps", ".queue {width:6em}", ".ui {width:8em}");


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-common/src/main/java/org/apache/hadoop/yarn/server/webapp/WebPageUtils.java
    return appsTableInit(false, true);
  }

  public static String appsTableInit(boolean isResourceManager) {
    return appsTableInit(false, isResourceManager);
  }

  public static String appsTableInit(
      boolean isFairSchedulerPage, boolean isResourceManager) {

