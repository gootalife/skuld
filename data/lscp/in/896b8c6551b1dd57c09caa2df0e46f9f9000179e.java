hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/main/java/org/apache/hadoop/yarn/util/ResourceCalculatorPlugin.java
package org.apache.hadoop.yarn.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
@InterfaceAudience.LimitedPrivate({"YARN", "MAPREDUCE"})
@InterfaceStability.Unstable
public class ResourceCalculatorPlugin extends Configured {
  private static final Log LOG =
      LogFactory.getLog(ResourceCalculatorPlugin.class);

  private final SysInfo sys;

    }
    try {
      return new ResourceCalculatorPlugin();
    } catch (Throwable t) {
      LOG.warn(t + ": Failed to instantiate default resource calculator.", t);
    }
    return null;
  }

}

