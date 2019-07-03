hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-common/src/main/java/org/apache/hadoop/yarn/server/webapp/WebPageUtils.java
       .append("{'sType':'string', 'aTargets': [0]")
       .append(", 'mRender': parseHadoopID }")
       .append("\n, {'sType':'numeric', 'aTargets': " +
           (isFairSchedulerPage ? "[6, 7]": "[6, 7]"))
       .append(", 'mRender': renderHadoopDate }")
       .append("\n, {'sType':'numeric', bSearchable:false, 'aTargets':");
     if (isFairSchedulerPage) {
       sb.append("[13]");
     } else if (isResourceManager) {
       sb.append("[13]");
     } else {
       sb.append("[9]");
     }

