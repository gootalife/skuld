hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/balancer/Dispatcher.java
        sendRequest(out, eb, accessToken);
        receiveResponse(in);
        nnc.getBytesMoved().addAndGet(block.getNumBytes());
        target.getDDatanode().setHasSuccess();
        LOG.info("Successfully moved " + this);
      } catch (IOException e) {
        LOG.warn("Failed to move " + this + ": " + e.getMessage());
    private final List<PendingMove> pendings;
    private volatile boolean hasFailure = false;
    private volatile boolean hasSuccess = false;
    private final int maxConcurrentMoves;

    @Override
    void setHasFailure() {
      this.hasFailure = true;
    }

    void setHasSuccess() {
      this.hasSuccess = true;
    }
  }

    }
  }

  public static boolean checkForSuccess(
      Iterable<? extends StorageGroup> targets) {
    boolean hasSuccess = false;
    for (StorageGroup t : targets) {
      hasSuccess |= t.getDDatanode().hasSuccess;
    }
    return hasSuccess;
  }


hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/mover/Mover.java
      boolean hasFailed = Dispatcher.waitForMoveCompletion(storages.targets
          .values());
      boolean hasSuccess = Dispatcher.checkForSuccess(storages.targets
          .values());
      if (hasFailed && !hasSuccess) {
        if (retryCount.get() == retryMaxAttempts) {
          result.setRetryFailed();
          LOG.error("Failed to move some block's after "
              + retryMaxAttempts + " retries.");
          return result;
        } else {
          retryCount.incrementAndGet();
        }

    private boolean hasRemaining;
    private boolean noBlockMoved;
    private boolean retryFailed;

    Result() {
      hasRemaining = false;
      noBlockMoved = true;
      retryFailed = false;
    }

    boolean isHasRemaining() {
      this.noBlockMoved = noBlockMoved;
    }

    void setRetryFailed() {
      this.retryFailed = true;
    }

    ExitStatus getExitStatus() {
      if (retryFailed) {
        return ExitStatus.NO_MOVE_PROGRESS;
      } else {
        return !isHasRemaining() ? ExitStatus.SUCCESS
            : isNoBlockMoved() ? ExitStatus.NO_MOVE_BLOCK
                : ExitStatus.IN_PROGRESS;
      }
    }

  }

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/mover/TestMover.java
      int rc = ToolRunner.run(conf, new Mover.Cli(),
          new String[] {"-p", file.toString()});
      Assert.assertEquals("Movement should fail after some retry",
          ExitStatus.NO_MOVE_PROGRESS.getExitCode(), rc);
    } finally {
      cluster.shutdown();
    }

