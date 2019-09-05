hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/fs/FsShell.java
import org.apache.hadoop.fs.shell.CommandFactory;
import org.apache.hadoop.fs.shell.FsCommand;
import org.apache.hadoop.tools.TableListing;
import org.apache.hadoop.tracing.TraceUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.htrace.Sampler;
import org.apache.htrace.SamplerBuilder;
import org.apache.htrace.Trace;
import org.apache.htrace.TraceScope;

@InterfaceAudience.Private
  private FileSystem fs;
  private Trash trash;
  protected CommandFactory commandFactory;
  private Sampler traceSampler;

  private final String usagePrefix =
    "Usage: hadoop fs [generic options]";
  public int run(String argv[]) throws Exception {
    init();
    traceSampler = new SamplerBuilder(TraceUtils.
        wrapHadoopConf("dfs.shell.htrace.", getConf())).build();
    int exitCode = -1;
    if (argv.length < 1) {
      printUsage(System.err);
        if (instance == null) {
          throw new UnknownCommandException();
        }
        TraceScope scope = Trace.startSpan(instance.getCommandName(), traceSampler);
        try {
          exitCode = instance.run(Arrays.copyOfRange(argv, 1, argv.length));
        } finally {
          scope.close();
        }
      } catch (IllegalArgumentException e) {
        displayError(cmd, e.getLocalizedMessage());
        if (instance != null) {

