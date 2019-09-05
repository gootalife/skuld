hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-app/src/main/java/org/apache/hadoop/mapreduce/v2/app/webapp/JobBlock.java
    JobInfo jinfo = new JobInfo(job, true);
    info("Job Overview").
        _("Job Name:", jinfo.getName()).
        _("User Name:", jinfo.getUserName()).
        _("Queue Name:", jinfo.getQueueName()).
        _("State:", jinfo.getState()).
        _("Uberized:", jinfo.isUberized()).
        _("Started:", new Date(jinfo.getStartTime())).

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-app/src/main/java/org/apache/hadoop/mapreduce/v2/app/webapp/dao/JobInfo.java
  protected String id;
  protected String name;
  protected String user;
  protected String queue;
  protected JobState state;
  protected int mapsTotal;
  protected int mapsCompleted;
    }
    this.name = job.getName().toString();
    this.user = job.getUserName();
    this.queue = job.getQueueName();
    this.state = job.getState();
    this.mapsTotal = job.getTotalMaps();
    this.mapsCompleted = job.getCompletedMaps();
    return this.name;
  }

  public String getQueueName() {
    return this.queue;
  }

  public String getId() {
    return this.id;
  }

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-app/src/test/java/org/apache/hadoop/mapreduce/v2/app/webapp/TestAMWebServicesJobs.java

  public void verifyAMJob(JSONObject info, Job job) throws JSONException {

    assertEquals("incorrect number of elements", 31, info.length());

    verifyAMJobGeneric(job, info.getString("id"), info.getString("user"),

