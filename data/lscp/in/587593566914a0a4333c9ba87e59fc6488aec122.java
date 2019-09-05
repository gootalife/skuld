hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/fs/FsShell.java
import org.apache.hadoop.fs.shell.Command;
import org.apache.hadoop.fs.shell.CommandFactory;
import org.apache.hadoop.fs.shell.FsCommand;
import org.apache.hadoop.tracing.SpanReceiverHost;
import org.apache.hadoop.tools.TableListing;
import org.apache.hadoop.tracing.TraceUtils;
import org.apache.hadoop.util.Tool;
  private final String usagePrefix =
    "Usage: hadoop fs [generic options]";

  private SpanReceiverHost spanReceiverHost;
  static final String SEHLL_HTRACE_PREFIX = "dfs.shell.htrace.";

      commandFactory.addObject(new Usage(), "-usage");
      registerCommands(commandFactory);
    }
    this.spanReceiverHost =
        SpanReceiverHost.get(getConf(), SEHLL_HTRACE_PREFIX);
  }

  protected void registerCommands(CommandFactory factory) {
    init();
    traceSampler = new SamplerBuilder(TraceUtils.
        wrapHadoopConf(SEHLL_HTRACE_PREFIX, getConf())).build();
    int exitCode = -1;
    if (argv.length < 1) {
      printUsage(System.err);
      fs.close();
      fs = null;
    }
    if (this.spanReceiverHost != null) {
      this.spanReceiverHost.closeReceivers();
    }
  }


hadoop-common-project/hadoop-common/src/test/java/org/apache/hadoop/fs/TestFsShell.java
package org.apache.hadoop.fs;

import junit.framework.AssertionFailedError;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tracing.SetSpanReceiver;
import org.apache.hadoop.tracing.SpanReceiverHost;
import org.apache.hadoop.util.ToolRunner;
import org.apache.htrace.SamplerBuilder;
import org.apache.htrace.impl.AlwaysSampler;
import org.junit.Test;

public class TestFsShell {
    }
  }

  @Test
  public void testTracing() throws Throwable {
    Configuration conf = new Configuration();
    String prefix = FsShell.SEHLL_HTRACE_PREFIX;
    conf.set(prefix + SpanReceiverHost.SPAN_RECEIVERS_CONF_SUFFIX,
        SetSpanReceiver.class.getName());
    conf.set(prefix + SamplerBuilder.SAMPLER_CONF_KEY,
        AlwaysSampler.class.getName());
    conf.setQuietMode(false);
    FsShell shell = new FsShell(conf);
    int res;
    try {
      res = ToolRunner.run(shell, new String[]{"-help"});
    } finally {
      shell.close();
    }
    SetSpanReceiver.assertSpanNamesFound(new String[]{"help"});
  }
}

hadoop-common-project/hadoop-common/src/test/java/org/apache/hadoop/tracing/SetSpanReceiver.java
    }
  }

  public static void assertSpanNamesFound(final String[] expectedSpanNames) {
    try {
      GenericTestUtils.waitFor(new Supplier<Boolean>() {
        @Override

