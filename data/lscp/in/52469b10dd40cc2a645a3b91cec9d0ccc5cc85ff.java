hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-app/src/main/java/org/apache/hadoop/mapreduce/v2/app/rm/ResourceCalculatorUtils.java
  public static int computeAvailableContainers(Resource available,
      Resource required, EnumSet<SchedulerResourceTypes> resourceTypes) {
    if (resourceTypes.contains(SchedulerResourceTypes.CPU)) {
      return Math.min(
        calculateRatioOrMaxValue(available.getMemory(), required.getMemory()),
        calculateRatioOrMaxValue(available.getVirtualCores(), required
            .getVirtualCores()));
    }
    return calculateRatioOrMaxValue(
      available.getMemory(), required.getMemory());
  }

  public static int divideAndCeilContainers(Resource required, Resource factor,
    }
    return divideAndCeil(required.getMemory(), factor.getMemory());
  }

  private static int calculateRatioOrMaxValue(int numerator, int denominator) {
    if (denominator == 0) {
      return Integer.MAX_VALUE;
    }
    return numerator / denominator;
  }
}

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-app/src/test/java/org/apache/hadoop/mapreduce/v2/app/rm/TestResourceCalculatorUtils.java
++ b/hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-app/src/test/java/org/apache/hadoop/mapreduce/v2/app/rm/TestResourceCalculatorUtils.java

package org.apache.hadoop.mapreduce.v2.app.rm;

import org.apache.hadoop.yarn.api.records.Resource;
import org.junit.Assert;
import org.junit.Test;

import java.util.EnumSet;

import static org.apache.hadoop.yarn.proto.YarnServiceProtos.*;

public class TestResourceCalculatorUtils {
  @Test
  public void testComputeAvailableContainers() throws Exception {
    Resource clusterAvailableResources = Resource.newInstance(81920, 40);

    Resource nonZeroResource = Resource.newInstance(1024, 2);

    int expectedNumberOfContainersForMemory = 80;
    int expectedNumberOfContainersForCPU = 20;

    verifyDifferentResourceTypes(clusterAvailableResources, nonZeroResource,
        expectedNumberOfContainersForMemory,
        expectedNumberOfContainersForCPU);

    Resource zeroMemoryResource = Resource.newInstance(0,
        nonZeroResource.getVirtualCores());

    verifyDifferentResourceTypes(clusterAvailableResources, zeroMemoryResource,
        Integer.MAX_VALUE,
        expectedNumberOfContainersForCPU);

    Resource zeroCpuResource = Resource.newInstance(nonZeroResource.getMemory(),
        0);

    verifyDifferentResourceTypes(clusterAvailableResources, zeroCpuResource,
        expectedNumberOfContainersForMemory,
        expectedNumberOfContainersForMemory);
  }

  private void verifyDifferentResourceTypes(Resource clusterAvailableResources,
      Resource nonZeroResource, int expectedNumberOfContainersForMemoryOnly,
      int expectedNumberOfContainersOverall) {

    Assert.assertEquals("Incorrect number of available containers for Memory",
        expectedNumberOfContainersForMemoryOnly,
        ResourceCalculatorUtils.computeAvailableContainers(
            clusterAvailableResources, nonZeroResource,
            EnumSet.of(SchedulerResourceTypes.MEMORY)));

    Assert.assertEquals("Incorrect number of available containers overall",
        expectedNumberOfContainersOverall,
        ResourceCalculatorUtils.computeAvailableContainers(
            clusterAvailableResources, nonZeroResource,
            EnumSet.of(SchedulerResourceTypes.CPU,
                SchedulerResourceTypes.MEMORY)));
  }
}

