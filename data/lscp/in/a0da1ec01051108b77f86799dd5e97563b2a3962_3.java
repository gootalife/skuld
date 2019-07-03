hadoop-yarn-project/hadoop-yarn/hadoop-yarn-client/src/test/java/org/apache/hadoop/yarn/client/cli/TestYarnCLI.java
           FinalApplicationStatus.SUCCEEDED, usageReport, "N/A", 0.53789f, "YARN",
           null, null, false);
       newApplicationReport.setLogAggregationStatus(LogAggregationStatus.SUCCEEDED);
       newApplicationReport.setPriority(Priority.newInstance(0));
       when(client.getApplicationReport(any(ApplicationId.class))).thenReturn(
           newApplicationReport);
       int result = cli.run(new String[] { "application", "-status", applicationId.toString() });
       pw.println("\tApplication-Type : YARN");
       pw.println("\tUser : user");
       pw.println("\tQueue : queue");
       pw.println("\tApplication Priority : 0");
       pw.println("\tStart-Time : 0");
       pw.println("\tFinish-Time : 0");
       pw.println("\tProgress : 53.79%");

