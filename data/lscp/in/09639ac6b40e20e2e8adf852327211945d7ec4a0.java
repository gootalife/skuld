hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/main/java/org/apache/hadoop/yarn/logaggregation/AggregatedLogFormat.java
    public String getApplicationOwner() throws IOException {
      TFile.Reader.Scanner ownerScanner = null;
      try {
        ownerScanner = reader.createScanner();
        LogKey key = new LogKey();
        while (!ownerScanner.atEnd()) {
          TFile.Reader.Scanner.Entry entry = ownerScanner.entry();
          ownerScanner.advance();
        }
        return null;
      } finally {
        IOUtils.cleanup(LOG, ownerScanner);
      }
    }

    public Map<ApplicationAccessType, String> getApplicationAcls()
        throws IOException {
      TFile.Reader.Scanner aclScanner = null;
      try {
        aclScanner = reader.createScanner();
        LogKey key = new LogKey();
        Map<ApplicationAccessType, String> acls =
            new HashMap<ApplicationAccessType, String>();
          aclScanner.advance();
        }
        return acls;
      } finally {
        IOUtils.cleanup(LOG, aclScanner);
      }
    }


