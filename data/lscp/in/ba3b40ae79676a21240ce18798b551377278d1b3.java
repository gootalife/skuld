hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/recovery/TestProtos.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/recovery/TestProtos.java

package org.apache.hadoop.yarn.server.resourcemanager.recovery;

import org.apache.hadoop.yarn.proto.YarnServerResourceManagerRecoveryProtos.EpochProto;
import org.junit.Assert;
import org.junit.Test;

public class TestProtos {

  @Test
  public void testProtoCanBePrinted() throws Exception {
    EpochProto proto = EpochProto.newBuilder().setEpoch(100).build();
    String protoString = proto.toString();
    Assert.assertNotNull(protoString);
  }
}

