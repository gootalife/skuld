hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/WindowsSecureContainerExecutor.java
        
        @Override
        public void run() {
          try (BufferedReader lines = new BufferedReader(
                   new InputStreamReader(stream, Charset.forName("UTF-8")))) {
            char[] buf = new char[512];
            int nRead;
            while ((nRead = lines.read(buf, 0, buf.length)) > 0) {
              output.append(buf, 0, nRead);
            }
          } catch (Throwable t) {
            LOG.error("Error occured reading the process stdout", t);
          }
        }

