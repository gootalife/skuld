hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/main/java/org/apache/hadoop/yarn/util/resource/Resources.java

    @Override
    public void setMemory(int memory) {
      throw new RuntimeException("UNBOUNDED cannot be modified!");
    }

    @Override

    @Override
    public void setVirtualCores(int cores) {
      throw new RuntimeException("UNBOUNDED cannot be modified!");
    }

    @Override
    public int compareTo(Resource o) {
      int diff = Integer.MAX_VALUE - o.getMemory();
      if (diff == 0) {
        diff = Integer.MAX_VALUE - o.getVirtualCores();
      }
      return diff;
    }

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/test/java/org/apache/hadoop/yarn/util/resource/TestResources.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/test/java/org/apache/hadoop/yarn/util/resource/TestResources.java

package org.apache.hadoop.yarn.util.resource;

import org.apache.hadoop.yarn.api.records.Resource;
import org.junit.Test;
import static org.junit.Assert.assertTrue;

public class TestResources {
  
  public Resource createResource(int memory, int vCores) {
    return Resource.newInstance(memory, vCores);
  }

  @Test(timeout=1000)
  public void testCompareToWithUnboundedResource() {
    assertTrue(Resources.unbounded().compareTo(
            createResource(Integer.MAX_VALUE, Integer.MAX_VALUE)) == 0);
    assertTrue(Resources.unbounded().compareTo(
        createResource(Integer.MAX_VALUE, 0)) > 0);
    assertTrue(Resources.unbounded().compareTo(
        createResource(0, Integer.MAX_VALUE)) > 0);
  }

  @Test(timeout=1000)
  public void testCompareToWithNoneResource() {
    assertTrue(Resources.none().compareTo(createResource(0, 0)) == 0);
    assertTrue(Resources.none().compareTo(
        createResource(1, 0)) < 0);
    assertTrue(Resources.none().compareTo(
        createResource(0, 1)) < 0);
  }
  
}

