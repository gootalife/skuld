hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/fs/FsShell.java
import org.apache.hadoop.tracing.SpanReceiverHost;
import org.apache.hadoop.tools.TableListing;
import org.apache.hadoop.tracing.TraceUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.htrace.Sampler;
          throw new UnknownCommandException();
        }
        TraceScope scope = Trace.startSpan(instance.getCommandName(), traceSampler);
        if (scope.getSpan() != null) {
          String args = StringUtils.join(" ", argv);
          if (args.length() > 2048) {
            args = args.substring(0, 2048);
          }
          scope.getSpan().addKVAnnotation("args", args);
        }
        try {
          exitCode = instance.run(Arrays.copyOfRange(argv, 1, argv.length));
        } finally {

hadoop-common-project/hadoop-common/src/test/java/org/apache/hadoop/fs/TestFsShell.java
import org.apache.hadoop.util.ToolRunner;
import org.apache.htrace.SamplerBuilder;
import org.apache.htrace.impl.AlwaysSampler;
import org.junit.Assert;
import org.junit.Test;

public class TestFsShell {
    FsShell shell = new FsShell(conf);
    int res;
    try {
      res = ToolRunner.run(shell, new String[]{"-help", "ls", "cat"});
    } finally {
      shell.close();
    }
    SetSpanReceiver.assertSpanNamesFound(new String[]{"help"});
    Assert.assertEquals("-help ls cat",
        SetSpanReceiver.getMap()
            .get("help").get(0).getKVAnnotations().get("args"));
  }
}

