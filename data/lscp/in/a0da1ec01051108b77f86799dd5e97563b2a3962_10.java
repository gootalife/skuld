hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/webapp/RMAppsBlock.java
     TBODY<TABLE<Hamlet>> tbody =
         html.table("#apps").thead().tr().th(".id", "ID").th(".user", "User")
           .th(".name", "Name").th(".type", "Application Type")
           .th(".queue", "Queue").th(".priority", "Application Priority")
           .th(".starttime", "StartTime")
           .th(".finishtime", "FinishTime").th(".state", "State")
           .th(".finalstatus", "FinalStatus")
           .th(".runningcontainer", "Running Containers")
         .append("\",\"")
         .append(
           StringEscapeUtils.escapeJavaScript(StringEscapeUtils.escapeHtml(app
              .getQueue()))).append("\",\"").append(String
              .valueOf(app.getPriority()))
         .append("\",\"").append(app.getStartedTime())
         .append("\",\"").append(app.getFinishedTime())
         .append("\",\"")
         .append(app.getAppState() == null ? UNAVAILABLE : app.getAppState())

