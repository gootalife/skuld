hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/main/java/org/apache/hadoop/yarn/api/records/impl/pb/SerializedExceptionPBImpl.java
    viaProto = false;
  }

  private static <T extends Throwable> T instantiateExceptionImpl(
      String message, Class<? extends T> cls, Throwable cause)
      throws NoSuchMethodException, InstantiationException,
      IllegalAccessException, InvocationTargetException {
    Constructor<? extends T> cn;
    T ex = null;
    cn =
        cls.getConstructor(message == null ? new Class[0]
            : new Class[] {String.class});
    cn.setAccessible(true);
    ex = message == null ? cn.newInstance() : cn.newInstance(message);
    ex.initCause(cause);
    return ex;
  }

  private static <T extends Throwable> T instantiateException(
      Class<? extends T> cls, String message, Throwable cause) {
    T ex = null;
    try {
      try {
        ex = instantiateExceptionImpl(message, cls, cause);
      } catch (NoSuchMethodException e) {
        ex = instantiateExceptionImpl(null, cls, cause);
      }
    } catch (SecurityException e) {
      throw new YarnRuntimeException(e);
    } catch (NoSuchMethodException e) {

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/test/java/org/apache/hadoop/yarn/api/records/impl/pb/TestSerializedExceptionPBImpl.java

package org.apache.hadoop.yarn.api.records.impl.pb;

import java.nio.channels.ClosedChannelException;

import org.junit.Assert;
import org.apache.hadoop.yarn.api.records.impl.pb.SerializedExceptionPBImpl;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
    Assert.assertEquals(ex.toString(), pb.deSerialize().toString());
  }

  @Test
  public void testDeserializeWithDefaultConstructor() {
    ClosedChannelException ex = new ClosedChannelException();
    SerializedExceptionPBImpl pb = new SerializedExceptionPBImpl();
    pb.init(ex);
    Assert.assertEquals(ex.getClass(), pb.deSerialize().getClass());
  }

  @Test
  public void testBeforeInit() throws Exception {
    SerializedExceptionProto defaultProto =

