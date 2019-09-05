hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/java/org/apache/hadoop/mapreduce/util/CountersStrings.java
  public static <C extends Counter, G extends CounterGroupBase<C>,
                 T extends AbstractCounters<C, G>>
  String toEscapedCompactString(T counters) {
    StringBuilder builder = new StringBuilder();
    synchronized(counters) {
      for (G group : counters) {
        builder.append(toEscapedCompactString(group));
      }
    }
    return builder.toString();
  }

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/test/java/org/apache/hadoop/mapred/TestCounters.java
package org.apache.hadoop.mapred;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
  private void testCounter(Counters counter) throws ParseException {
    String compactEscapedString = counter.makeEscapedCompactString();
    assertFalse("compactEscapedString should not contain null",
                compactEscapedString.contains("null"));
    
    Counters recoveredCounter = 
      Counters.fromEscapedCompactString(compactEscapedString);

