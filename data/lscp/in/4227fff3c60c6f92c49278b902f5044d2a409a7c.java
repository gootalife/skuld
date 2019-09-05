hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/fs/FileContext.java
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.ShutdownHookManager;

import com.google.common.base.Preconditions;

  Path fixRelativePart(Path p) {
    Preconditions.checkNotNull(p, "path cannot be null");
    if (p.isUriPathAbsolute()) {
      return p;
    } else {

