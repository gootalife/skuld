hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/monitor/ContainersMonitorImpl.java

        long vmemUsageByAllContainers = 0;
        long pmemByAllContainers = 0;
        long cpuUsagePercentPerCoreByAllContainers = 0;
        long cpuUsageTotalCoresByAllContainers = 0;
        for (Iterator<Map.Entry<ContainerId, ProcessTreeInfo>> it =
            trackingContainers.entrySet().iterator(); it.hasNext();) {

              containerExitStatus = ContainerExitStatus.KILLED_EXCEEDED_PMEM;
            }

            vmemUsageByAllContainers += currentVmemUsage;
            pmemByAllContainers += currentPmemUsage;
            cpuUsagePercentPerCoreByAllContainers += cpuUsagePercentPerCore;
            cpuUsageTotalCoresByAllContainers += cpuUsagePercentPerCore;

            if (isMemoryOverLimit) {
                      containerExitStatus, msg));
              it.remove();
              LOG.info("Removed ProcessTree with root " + pId);
            }
          } catch (Exception e) {
                + "while managing memory of " + containerId, e);
          }
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug("Total Resource Usage stats in NM by all containers : "
              + "Virtual Memory= " + vmemUsageByAllContainers
              + ", Physical Memory= " + pmemByAllContainers
              + ", Total CPU usage= " + cpuUsageTotalCoresByAllContainers
              + ", Total CPU(% per core) usage"
              + cpuUsagePercentPerCoreByAllContainers);
        }

        try {
          Thread.sleep(monitoringInterval);

