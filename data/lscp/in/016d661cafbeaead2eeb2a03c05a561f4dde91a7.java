hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/fs/FileSystem.java
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.util.StringUtils;
import org.apache.htrace.Span;
import org.apache.htrace.Trace;
import org.apache.htrace.TraceScope;

import com.google.common.annotations.VisibleForTesting;


  private static FileSystem createFileSystem(URI uri, Configuration conf
      ) throws IOException {
    TraceScope scope = Trace.startSpan("FileSystem#createFileSystem");
    Span span = scope.getSpan();
    if (span != null) {
      span.addKVAnnotation("scheme", uri.getScheme());
    }
    try {
      Class<?> clazz = getFileSystemClass(uri.getScheme(), conf);
      FileSystem fs = (FileSystem)ReflectionUtils.newInstance(clazz, conf);
      fs.initialize(uri, conf);
      return fs;
    } finally {
      scope.close();
    }
  }


hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/fs/Globber.java
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import org.apache.htrace.Span;
import org.apache.htrace.Trace;
import org.apache.htrace.TraceScope;

@InterfaceAudience.Private
@InterfaceStability.Unstable
class Globber {
  }

  public FileStatus[] glob() throws IOException {
    TraceScope scope = Trace.startSpan("Globber#glob");
    Span span = scope.getSpan();
    if (span != null) {
      span.addKVAnnotation("pattern", pathPattern.toUri().getPath());
    }
    try {
      return doGlob();
    } finally {
      scope.close();
    }
  }

  private FileStatus[] doGlob() throws IOException {
    String scheme = schemeFromPath(pathPattern);

