hadoop-common-project/hadoop-common/src/test/java/org/apache/hadoop/util/TestStringUtils.java

import org.apache.hadoop.test.UnitTestcaseTimeLimit;
import org.apache.hadoop.util.StringUtils.TraditionalBinaryPrefix;
import org.junit.Assume;
import org.junit.Test;

public class TestStringUtils extends UnitTestcaseTimeLimit {

  @Test
  public void testLowerAndUpperStrings() {
    Assume.assumeTrue(Shell.LINUX);
    Locale defaultLocale = Locale.getDefault();
    try {
      Locale.setDefault(new Locale("tr", "TR"));

