hadoop-tools/hadoop-distcp/src/main/java/org/apache/hadoop/tools/SimpleCopyListing.java
          }
        }
        result = new WorkReport<FileStatus[]>(
            fileSystem.listStatus(parent.getPath()), retry, true);
      } catch (FileNotFoundException fnf) {
        LOG.error("FileNotFoundException exception in listStatus: " +
                  fnf.getMessage());
        result = new WorkReport<FileStatus[]>(new FileStatus[0], retry, true,
                                              fnf);
      } catch (Exception e) {
        LOG.error("Exception in listStatus. Will send for retry.");
        FileStatus[] parentList = new FileStatus[1];

    for (FileStatus status : sourceDirs) {
      workers.put(new WorkRequest<FileStatus>(status, 0));
    }

    while (workers.hasWork()) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Recording source-path: " + child.getPath() + " for copy.");
          }
          if (workResult.getSuccess()) {
            CopyListingFileStatus childCopyListingStatus =
              DistCpUtils.toCopyListingFileStatus(sourceFS, child,
                preserveAcls && child.isDirectory(),
                LOG.debug("Traversing into source dir: " + child.getPath());
              }
              workers.put(new WorkRequest<FileStatus>(child, retry));
            }
          } else {
            LOG.error("Giving up on " + child.getPath() +
      totalDirs++;
    }
    totalPaths++;
    maybePrintStats();
  }
}

