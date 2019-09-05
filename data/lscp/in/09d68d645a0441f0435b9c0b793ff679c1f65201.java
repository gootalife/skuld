hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/fs/TrashPolicyDefault.java
    this.emptierInterval = (long)(conf.getFloat(
        FS_TRASH_CHECKPOINT_INTERVAL_KEY, FS_TRASH_CHECKPOINT_INTERVAL_DEFAULT)
   }

  private Path makeTrashRelativePath(Path basePath, Path rmFilePath) {
                 " minutes that is used for deletion instead");
        this.emptierInterval = deletionInterval;
      }
      LOG.info("Namenode trash configuration: Deletion interval = "
          + (deletionInterval / MSECS_PER_MINUTE)
          + " minutes, Emptier interval = "
          + (emptierInterval / MSECS_PER_MINUTE) + " minutes.");
    }

    @Override

