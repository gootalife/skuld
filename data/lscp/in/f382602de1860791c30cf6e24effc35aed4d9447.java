hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/main/java/org/apache/hadoop/yarn/webapp/view/TextView.java

import java.io.PrintWriter;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.yarn.webapp.View;

  public void echo(Object... args) {
    PrintWriter out = writer();
    for (Object s : args) {
      String escapedString = StringEscapeUtils.escapeJavaScript(
          StringEscapeUtils.escapeHtml(s.toString()));
      out.print(escapedString);
    }
  }


