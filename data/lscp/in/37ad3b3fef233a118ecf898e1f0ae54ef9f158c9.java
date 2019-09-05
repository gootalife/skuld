hadoop-mapreduce-project/hadoop-mapreduce-examples/src/main/java/org/apache/hadoop/examples/QuasiMonteCarlo.java
      System.out.println("Starting Job");
      final long startTime = System.currentTimeMillis();
      job.waitForCompletion(true);
      if (!job.isSuccessful()) {
        System.out.println("Job " + job.getJobID() + " failed!");
        System.exit(1);
      }
      final double duration = (System.currentTimeMillis() - startTime)/1000.0;
      System.out.println("Job Finished in " + duration + " seconds");


