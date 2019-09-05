hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/main/java/org/apache/hadoop/yarn/api/records/impl/pb/SerializedExceptionPBImpl.java
    } else if (RuntimeException.class.isAssignableFrom(realClass)) {
      classType = RuntimeException.class;
    } else {
      classType = Throwable.class;
    }
    return instantiateException(realClass.asSubclass(classType), getMessage(),
      cause == null ? null : cause.deSerialize());

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/test/java/org/apache/hadoop/yarn/api/records/impl/pb/TestSerializedExceptionPBImpl.java

import java.nio.channels.ClosedChannelException;

import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.proto.YarnProtos.SerializedExceptionProto;
import org.junit.Assert;
import org.junit.Test;

public class TestSerializedExceptionPBImpl {
    SerializedExceptionPBImpl pb3 = new SerializedExceptionPBImpl();
    Assert.assertEquals(defaultProto.getTrace(), pb3.getRemoteTrace());
  }

  @Test
  public void testThrowableDeserialization() {
    Error ex = new Error();
    SerializedExceptionPBImpl pb = new SerializedExceptionPBImpl();
    pb.init(ex);
    Assert.assertEquals(ex.getClass(), pb.deSerialize().getClass());
  }
}

