hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/main/java/org/apache/hadoop/yarn/webapp/view/TwoColumnLayout.java
    styles.add(join('#', tableId, "_paginate span {font-weight:normal}"));
    styles.add(join('#', tableId, " .progress {width:8em}"));
    styles.add(join('#', tableId, "_processing {top:-1.5em; font-size:1em;"));
    styles.add("  color:#000; background:#fefefe}");
    for (String style : innerStyles) {
      styles.add(join('#', tableId, " ", style));
    }

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/webapp/CapacitySchedulerPage.java
  static final float Q_MAX_WIDTH = 0.8f;
  static final float Q_STATS_POS = Q_MAX_WIDTH + 0.05f;
  static final String Q_END = "left:101%";
  static final String Q_GIVEN =
      "left:0%;background:none;border:1px dashed #BFBFBF";
  static final String Q_OVER = "background:#FFA333";
  static final String Q_UNDER = "background:#5BD75B";

  @RequestScoped
  static class CSQInfo {

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/webapp/DefaultSchedulerPage.java
  static final String _Q = ".ui-state-default.ui-corner-all";
  static final float WIDTH_F = 0.8f;
  static final String Q_END = "left:101%";
  static final String OVER = "font-size:1px;background:#FFA333";
  static final String UNDER = "font-size:1px;background:#5BD75B";
  static final float EPSILON = 1e-8f;

  static class QueueInfoBlock extends HtmlBlock {

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/webapp/FairSchedulerPage.java
  static final float Q_MAX_WIDTH = 0.8f;
  static final float Q_STATS_POS = Q_MAX_WIDTH + 0.05f;
  static final String Q_END = "left:101%";
  static final String Q_GIVEN =
      "left:0%;background:none;border:1px solid #000000";
  static final String Q_INSTANTANEOUS_FS =
      "left:0%;background:none;border:1px dashed #000000";
  static final String Q_OVER = "background:#FFA333";
  static final String Q_UNDER = "background:#5BD75B";
  static final String STEADY_FAIR_SHARE = "Steady Fair Share";
  static final String INSTANTANEOUS_FAIR_SHARE = "Instantaneous Fair Share";
  @RequestScoped

