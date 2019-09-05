hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSNamesystem.java
  void checkNameNodeSafeMode(String errorMsg)
      throws RetriableException, SafeModeException {
    if (isInSafeMode()) {
      SafeModeException se = newSafemodeException(errorMsg);
      if (haEnabled && haContext != null
          && haContext.getState().getServiceState() == HAServiceState.ACTIVE
          && shouldRetrySafeMode(this.safeMode)) {
    }
  }

  private SafeModeException newSafemodeException(String errorMsg) {
    return new SafeModeException(errorMsg + ". Name node is in safe " +
        "mode.\n" + safeMode.getTurnOffTip());
  }

  boolean isPermissionEnabled() {
    return isPermissionEnabled;
  }
      for (LocatedBlock b : ret.blocks.getLocatedBlocks()) {
        if ((b.getLocations() == null) || (b.getLocations().length == 0)) {
          SafeModeException se = newSafemodeException(
              "Zero blocklocations for " + src);
          if (haEnabled && haContext != null &&
              haContext.getState().getServiceState() == HAServiceState.ACTIVE) {
            throw new RetriableException(se);

hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/SafeModeException.java
hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/TestSafeMode.java
      fail(msg);
    } catch (RemoteException re) {
      assertEquals(SafeModeException.class.getName(), re.getClassName());
      GenericTestUtils.assertExceptionContains("Name node is in safe mode", re);
    } catch (SafeModeException ignored) {
    } catch (IOException ioe) {
      fail(msg + " " + StringUtils.stringifyException(ioe));
    }

