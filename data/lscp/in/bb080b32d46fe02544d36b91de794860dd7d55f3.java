hadoop-tools/hadoop-azure/src/main/java/org/apache/hadoop/fs/azure/NativeAzureFileSystem.java
      FileMetadata parentMetadata = store.retrieveMetadata(parentKey);
      if (parentMetadata != null && parentMetadata.isDir() &&
        parentMetadata.getBlobMaterialization() == BlobMaterialization.Explicit) {
        if (parentFolderLease != null) {
          store.updateFolderLastModifiedTime(parentKey, parentFolderLease);
        } else {
          updateParentFolderLastModifiedTime(key);
        }
      } else {

