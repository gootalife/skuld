hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/test/java/org/apache/hadoop/yarn/util/resource/TestResourceCalculator.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/test/java/org/apache/hadoop/yarn/util/resource/TestResourceCalculator.java

package org.apache.hadoop.yarn.util.resource;

import java.util.Arrays;
import java.util.Collection;

import org.apache.hadoop.yarn.api.records.Resource;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestResourceCalculator {
  private ResourceCalculator resourceCalculator;

  @Parameterized.Parameters
  public static Collection<ResourceCalculator[]> getParameters() {
    return Arrays.asList(new ResourceCalculator[][] {
        { new DefaultResourceCalculator() },
        { new DominantResourceCalculator() } });
  }

  public TestResourceCalculator(ResourceCalculator rs) {
    this.resourceCalculator = rs;
  }

  @Test(timeout = 10000)
  public void testResourceCalculatorCompareMethod() {
    Resource clusterResource = Resource.newInstance(0, 0);

    Resource lhs = Resource.newInstance(0, 0);
    Resource rhs = Resource.newInstance(0, 0);
    assertResourcesOperations(clusterResource, lhs, rhs, false, true, false,
        true, lhs, lhs);

    lhs = Resource.newInstance(1, 1);
    rhs = Resource.newInstance(0, 0);
    assertResourcesOperations(clusterResource, lhs, rhs, false, false, true,
        true, lhs, rhs);

    lhs = Resource.newInstance(0, 0);
    rhs = Resource.newInstance(1, 1);
    assertResourcesOperations(clusterResource, lhs, rhs, true, true, false,
        false, rhs, lhs);

    if (!(resourceCalculator instanceof DominantResourceCalculator)) {
      return;
    }

    lhs = Resource.newInstance(1, 0);
    rhs = Resource.newInstance(0, 1);
    assertResourcesOperations(clusterResource, lhs, rhs, false, true, false,
        true, lhs, lhs);

    lhs = Resource.newInstance(0, 1);
    rhs = Resource.newInstance(1, 0);
    assertResourcesOperations(clusterResource, lhs, rhs, false, true, false,
        true, lhs, lhs);

    lhs = Resource.newInstance(1, 1);
    rhs = Resource.newInstance(1, 0);
    assertResourcesOperations(clusterResource, lhs, rhs, false, false, true,
        true, lhs, rhs);

    lhs = Resource.newInstance(0, 1);
    rhs = Resource.newInstance(1, 1);
    assertResourcesOperations(clusterResource, lhs, rhs, true, true, false,
        false, rhs, lhs);

  }


  private void assertResourcesOperations(Resource clusterResource,
      Resource lhs, Resource rhs, boolean lessThan, boolean lessThanOrEqual,
      boolean greaterThan, boolean greaterThanOrEqual, Resource max,
      Resource min) {

    Assert.assertEquals("Less Than operation is wrongly calculated.", lessThan,
        Resources.lessThan(resourceCalculator, clusterResource, lhs, rhs));

    Assert.assertEquals(
        "Less Than Or Equal To operation is wrongly calculated.",
        lessThanOrEqual, Resources.lessThanOrEqual(resourceCalculator,
            clusterResource, lhs, rhs));

    Assert.assertEquals("Greater Than operation is wrongly calculated.",
        greaterThan,
        Resources.greaterThan(resourceCalculator, clusterResource, lhs, rhs));

    Assert.assertEquals(
        "Greater Than Or Equal To operation is wrongly calculated.",
        greaterThanOrEqual, Resources.greaterThanOrEqual(resourceCalculator,
            clusterResource, lhs, rhs));

    Assert.assertEquals("Max(value) Operation wrongly calculated.", max,
        Resources.max(resourceCalculator, clusterResource, lhs, rhs));

    Assert.assertEquals("Min(value) operation is wrongly calculated.", min,
        Resources.min(resourceCalculator, clusterResource, lhs, rhs));
  }

}
\No newline at end of file

