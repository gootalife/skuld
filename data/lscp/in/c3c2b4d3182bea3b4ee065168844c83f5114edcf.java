hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/ipc/Client.java
          }
        });
      } catch (ExecutionException e) {
        Throwable cause = e.getCause();
        if (cause instanceof IOException) {
          throw (IOException) cause;
        } else {
          throw new IOException(cause);
        }
      }
      if (connection.addCall(call)) {
        break;

