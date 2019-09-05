hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-app/src/main/java/org/apache/hadoop/mapreduce/v2/app/webapp/AppController.java
    set(APP_ID, app.context.getApplicationID().toString());
    set(RM_WEB,
        JOINER.join(MRWebAppUtil.getYARNWebappScheme(),
            WebAppUtils.getResolvedRemoteRMWebAppURLWithoutScheme(conf,
                MRWebAppUtil.getYARNHttpPolicy())));
  }


