hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/ipc/Client.java
          if (erCode == null) {
             LOG.warn("Detailed error code not set by server on rpc error");
          }
          RemoteException re = new RemoteException(exceptionClassName, errorMsg, erCode);
          if (status == RpcStatusProto.ERROR) {
            calls.remove(callId);
            call.setException(re);

hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/ipc/RemoteException.java
import org.xml.sax.Attributes;

public class RemoteException extends IOException {
  private static final int UNSPECIFIED_ERROR = -1;
  private static final long serialVersionUID = 1L;
  private final int errorCode;

  private final String className;
  
  public RemoteException(String className, String msg) {
    this(className, msg, null);
  }
  
  public RemoteException(String className, String msg, RpcErrorCodeProto erCode) {
    super(msg);
    this.className = className;
    if (erCode != null)
      errorCode = erCode.getNumber();
    else 
      errorCode = UNSPECIFIED_ERROR;
  }
  
  public String getClassName() {
    return className;
  }
  
  public RpcErrorCodeProto getErrorCode() {
    return RpcErrorCodeProto.valueOf(errorCode);
  }
  public IOException unwrapRemoteException(Class<?>... lookupTypes) {
    return ex;
  }

  public static RemoteException valueOf(Attributes attrs) {
    return new RemoteException(attrs.getValue("class"),
        attrs.getValue("message")); 

hadoop-common-project/hadoop-common/src/test/java/org/apache/hadoop/ipc/TestRPC.java
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.ipc.Client.ConnectionId;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.AccessControlException;
      }
    } catch (RemoteException e) {
      if (expectFailure) {
        assertEquals("RPC error code should be UNAUTHORIZED", RpcErrorCodeProto.FATAL_UNAUTHORIZED, e.getErrorCode());
        assertTrue(e.unwrapRemoteException() instanceof AuthorizationException);
      } else {
        throw e;
      proxy.echo("");
    } catch (RemoteException e) {
      LOG.info("LOGGING MESSAGE: " + e.getLocalizedMessage());
      assertEquals("RPC error code should be UNAUTHORIZED", RpcErrorCodeProto.FATAL_UNAUTHORIZED, e.getErrorCode());
      assertTrue(e.unwrapRemoteException() instanceof AccessControlException);
      succeeded = true;
    } finally {
      proxy.echo("");
    } catch (RemoteException e) {
      LOG.info("LOGGING MESSAGE: " + e.getLocalizedMessage());
      assertEquals("RPC error code should be UNAUTHORIZED", RpcErrorCodeProto.FATAL_UNAUTHORIZED, e.getErrorCode());
      assertTrue(e.unwrapRemoteException() instanceof AccessControlException);
      succeeded = true;
    } finally {

hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/ha/TestHASafeMode.java
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.StandbyException;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.log4j.Level;
import org.junit.After;
      fail("StandBy should throw exception for isInSafeMode");
    } catch (IOException e) {
      if (e instanceof RemoteException) {
        assertEquals("RPC Error code should indicate app failure.", RpcErrorCodeProto.ERROR_APPLICATION,
            ((RemoteException) e).getErrorCode());
        IOException sbExcpetion = ((RemoteException) e).unwrapRemoteException();
        assertTrue("StandBy nn should not support isInSafeMode",
            sbExcpetion instanceof StandbyException);

