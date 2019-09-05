hadoop-common-project/hadoop-common/src/test/java/org/apache/hadoop/cli/util/RegexpAcrossOutputComparator.java

package org.apache.hadoop.cli.util;

import org.apache.hadoop.util.Shell;
import java.util.regex.Pattern;


  @Override
  public boolean compare(String actual, String expected) {
    if (Shell.WINDOWS) {
      actual = actual.replaceAll("\\r", "");
      expected = expected.replaceAll("\\r", "");
    }
    return Pattern.compile(expected).matcher(actual).find();
  }


