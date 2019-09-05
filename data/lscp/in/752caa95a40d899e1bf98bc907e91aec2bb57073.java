hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/NodeManager.java
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.NodeHealthScriptRunner;
import org.apache.hadoop.util.ReflectionUtils;
  
  private AtomicBoolean isStopping = new AtomicBoolean(false);
  private boolean rmWorkPreservingRestartEnabled;
  private boolean shouldExitOnShutdownEvent = false;

  public NodeManager() {
    super(NodeManager.class.getName());
    new Thread() {
      @Override
      public void run() {
        try {
          NodeManager.this.stop();
        } catch (Throwable t) {
          LOG.error("Error while shutting down NodeManager", t);
        } finally {
          if (shouldExitOnShutdownEvent
              && !ShutdownHookManager.get().isShutdownInProgress()) {
            ExitUtil.terminate(-1);
          }
        }
      }
    }.start();
  }
      nodeManagerShutdownHook = new CompositeServiceShutdownHook(this);
      ShutdownHookManager.get().addShutdownHook(nodeManagerShutdownHook,
                                                SHUTDOWN_HOOK_PRIORITY);
      this.shouldExitOnShutdownEvent = true;
      this.init(conf);
      this.start();
    } catch (Throwable t) {

