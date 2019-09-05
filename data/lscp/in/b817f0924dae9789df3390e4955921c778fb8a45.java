hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-jobclient/src/test/java/org/apache/hadoop/mapred/pipes/TestPipeApplication.java
    } catch (ExitUtil.ExitException e) {
      assertTrue(out.toString().contains(""));
      assertTrue(out.toString(), out.toString().contains("pipes"));
      assertTrue(out.toString().contains("[-input <path>] // Input directory"));
      assertTrue(out.toString()
              .contains("[-output <path>] // Output directory"));

