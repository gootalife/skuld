hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/test/java/org/apache/hadoop/yarn/server/nodemanager/util/TestNodeManagerHardwareUtils.java
public class TestNodeManagerHardwareUtils {

  static class TestResourceCalculatorPlugin extends ResourceCalculatorPlugin {

    TestResourceCalculatorPlugin() {
      super(null);
    }

    @Override
    public long getVirtualMemorySize() {
      return 0;

