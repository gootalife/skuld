hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/util/Shell.java
      new String[] { "kill", "-" + code, isSetsidAvailable ? "-" + pid : pid };
  }

  public static final String ENV_NAME_REGEX = "[A-Za-z_][A-Za-z0-9_]*";
  public static String getEnvironmentVariableRegex() {
    return (WINDOWS)
        ? "%(" + ENV_NAME_REGEX + "?)%"
        : "\\$(" + ENV_NAME_REGEX + ")";
  }
  

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/main/java/org/apache/hadoop/yarn/util/Apps.java
public class Apps {
  public static final String APP = "application";
  public static final String ID = "ID";
  private static final Pattern VAR_SUBBER =
      Pattern.compile(Shell.getEnvironmentVariableRegex());
  private static final Pattern VARVAL_SPLITTER = Pattern.compile(
        "(?<=^|,)"                            // preceded by ',' or line begin
      + '(' + Shell.ENV_NAME_REGEX + ')'      // var group
      + '='
      + "([^,]*)"                             // val group
      );

  public static ApplicationId toAppID(String aid) {
    Iterator<String> it = _split(aid).iterator();
  public static void setEnvFromInputString(Map<String, String> env,
      String envString,  String classPathSeparator) {
    if (envString != null && envString.length() > 0) {
      Matcher varValMatcher = VARVAL_SPLITTER.matcher(envString);
      while (varValMatcher.find()) {
        String envVar = varValMatcher.group(1);
        Matcher m = VAR_SUBBER.matcher(varValMatcher.group(2));
        StringBuffer sb = new StringBuffer();
        while (m.find()) {
          String var = m.group(1);
          m.appendReplacement(sb, Matcher.quoteReplacement(replace));
        }
        m.appendTail(sb);
        addToEnvironment(env, envVar, sb.toString(), classPathSeparator);
      }
    }
  }

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/test/java/org/apache/hadoop/yarn/util/TestApps.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/test/java/org/apache/hadoop/yarn/util/TestApps.java
package org.apache.hadoop.yarn.util;

import org.apache.hadoop.util.Shell;
import org.junit.Test;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class TestApps {
  @Test
  public void testSetEnvFromInputString() {
    Map<String, String> environment = new HashMap<String, String>();
    environment.put("JAVA_HOME", "/path/jdk");
    String goodEnv = "a1=1,b_2=2,_c=3,d=4,e=,f_win=%JAVA_HOME%"
        + ",g_nix=$JAVA_HOME";
    Apps.setEnvFromInputString(environment, goodEnv, File.pathSeparator);
    assertEquals("1", environment.get("a1"));
    assertEquals("2", environment.get("b_2"));
    assertEquals("3", environment.get("_c"));
    assertEquals("4", environment.get("d"));
    assertEquals("", environment.get("e"));
    if (Shell.WINDOWS) {
      assertEquals("$JAVA_HOME", environment.get("g_nix"));
      assertEquals("/path/jdk", environment.get("f_win"));
    } else {
      assertEquals("/path/jdk", environment.get("g_nix"));
      assertEquals("%JAVA_HOME%", environment.get("f_win"));
    }
    String badEnv = "1,,2=a=b,3=a=,4==,5==a,==,c-3=3,=";
    environment.clear();
    Apps.setEnvFromInputString(environment, badEnv, File.pathSeparator);
    assertEquals(environment.size(), 0);

    environment.clear();
    Apps.setEnvFromInputString(environment, "b1,e1==,e2=a1=a2,b2",
        File.pathSeparator);
    assertEquals("=", environment.get("e1"));
    assertEquals("a1=a2", environment.get("e2"));
  }
}

