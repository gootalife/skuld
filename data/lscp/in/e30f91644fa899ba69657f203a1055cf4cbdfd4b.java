hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/ipc/Client.java
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.net.SocketFactory;
import javax.security.sasl.Sasl;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
    retryCount.set(rc);
  }

  private final Cache<ConnectionId, Connection> connections =
      CacheBuilder.newBuilder().build();

  private Class<? extends Writable> valueClass;   // class of call values
  private AtomicBoolean running = new AtomicBoolean(true); // if client runs
        return;
      }

      connections.invalidate(remoteId);

      IOUtils.closeStream(out);
    }
    
    for (Connection conn : connections.asMap().values()) {
      conn.interrupt();
    }
    
    while (connections.size() > 0) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
  public Writable call(Writable param, InetSocketAddress address)
      throws IOException {
    ConnectionId remoteId = ConnectionId.getConnectionId(address, null, null, 0,
        conf);
    return call(RpcKind.RPC_BUILTIN, param, remoteId);

  }

  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  Set<ConnectionId> getConnectionIds() {
    return connections.asMap().keySet();
  }
  
  private Connection getConnection(
      final ConnectionId remoteId,
      Call call, final int serviceClass, AtomicBoolean fallbackToSimpleAuth)
      throws IOException {
    if (!running.get()) {
    while(true) {
      try {
        connection = connections.get(remoteId, new Callable<Connection>() {
          @Override
          public Connection call() throws Exception {
            return new Connection(remoteId, serviceClass);
          }
        });
      } catch (ExecutionException e) {
        throw new IOException(e);
      }
      if (connection.addCall(call)) {
        break;
      } else {
        connections.invalidate(remoteId);
      }
    }
    

