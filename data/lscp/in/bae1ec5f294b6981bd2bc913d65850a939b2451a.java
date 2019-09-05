hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-hs/src/main/java/org/apache/hadoop/mapreduce/v2/hs/webapp/HsJobBlock.java
      for(String diag: diagnostics) {
        b.append(addTaskLinks(diag));
      }
      infoBlock._r("Diagnostics:", b.toString());
    }

    if(job.getNumMaps() > 0) {

