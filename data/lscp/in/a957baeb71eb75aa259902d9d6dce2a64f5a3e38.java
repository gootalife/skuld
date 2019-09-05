hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/java/org/apache/hadoop/mapreduce/task/reduce/Fetcher.java
    LOG.debug("url="+msgToEncode+";encHash="+encHash+";replyHash="+replyHash);
    SecureShuffleUtils.verifyReply(replyHash, encHash, shuffleSecretKey);
    LOG.debug("for url="+msgToEncode+" sent hash and received reply");
  }

  private void setupShuffleConnection(String encHash) {

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/java/org/apache/hadoop/mapreduce/task/reduce/ShuffleSchedulerImpl.java
      pendingHosts.remove(host);
      host.markBusy();

      LOG.debug("Assigning " + host + " with " + host.getNumKnownMapOutputs() +
               " to " + Thread.currentThread().getName());
      shuffleStart.set(Time.monotonicNow());

        host.addKnownMap(id);
      }
    }
    LOG.debug("assigned " + includedMaps + " of " + totalSize + " to " +
             host + " to " + Thread.currentThread().getName());
    return result;
  }

