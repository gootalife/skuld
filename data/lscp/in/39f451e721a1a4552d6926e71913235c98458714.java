hadoop-tools/hadoop-azure/src/main/java/org/apache/hadoop/fs/azure/AzureNativeFileSystemStore.java
      CloudBlobWrapper dstBlob = getBlobReference(dstKey);

      try {
        dstBlob.startCopyFromBlob(srcBlob, null, getInstrumentedContext());
      } catch (StorageException se) {
        if (se.getErrorCode().equals(
		  StorageErrorCode.SERVER_BUSY.toString())) {
          options.setRetryPolicyFactory(new RetryExponentialRetry(
            copyBlobMinBackoff, copyBlobDeltaBackoff, copyBlobMaxBackoff, 
			copyBlobMaxRetries));
          dstBlob.startCopyFromBlob(srcBlob, options, getInstrumentedContext());
        } else {
          throw se;
        }

hadoop-tools/hadoop-azure/src/main/java/org/apache/hadoop/fs/azure/StorageInterface.java
    public abstract void startCopyFromBlob(CloudBlobWrapper sourceBlob,
        BlobRequestOptions options, OperationContext opContext)
        throws StorageException, URISyntaxException;
    

hadoop-tools/hadoop-azure/src/main/java/org/apache/hadoop/fs/azure/StorageInterfaceImpl.java
    }

    @Override
    public void startCopyFromBlob(CloudBlobWrapper sourceBlob, BlobRequestOptions options,
        OperationContext opContext)
            throws StorageException, URISyntaxException {
      getBlob().startCopyFromBlob(((CloudBlobWrapperImpl)sourceBlob).blob,
          null, null, options, opContext);
    }


hadoop-tools/hadoop-azure/src/test/java/org/apache/hadoop/fs/azure/MockStorageInterface.java
    }

    @Override
    public void startCopyFromBlob(CloudBlobWrapper sourceBlob, BlobRequestOptions options,
        OperationContext opContext) throws StorageException, URISyntaxException {
      backingStore.copy(convertUriToDecodedString(sourceBlob.getUri()), convertUriToDecodedString(uri));
    }

hadoop-tools/hadoop-azure/src/test/java/org/apache/hadoop/fs/azure/TestAzureFileSystemErrorConditions.java
        @Override
        public boolean isTargetConnection(HttpURLConnection connection) {
          return connection.getRequestMethod().equals("PUT")
              && connection.getURL().getQuery() != null
              && connection.getURL().getQuery().contains("blocklist");
        }
      });

hadoop-tools/hadoop-azure/src/test/java/org/apache/hadoop/fs/azure/TestBlobDataValidation.java

    private static boolean isPutBlock(HttpURLConnection connection) {
      return connection.getRequestMethod().equals("PUT")
          && connection.getURL().getQuery() != null
          && connection.getURL().getQuery().contains("blockid");
    }


