hadoop-tools/hadoop-azure/src/main/java/org/apache/hadoop/fs/azure/NativeAzureFileSystem.java
              createPermissionStatus(FsPermission.getDefault()));
        } else {
          if (!skipParentFolderLastModifidedTimeUpdate) {
            updateParentFolderLastModifiedTime(key);
          }
        }
      }
      Path parent = absolutePath.getParent();
      if (parent != null && parent.getParent() != null) { // not root
        if (!skipParentFolderLastModifidedTimeUpdate) {
          updateParentFolderLastModifiedTime(key);
        }
      }
      instrumentation.directoryDeleted();

