hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/conf/Configuration.java
  private void loadProperty(Properties properties, String name, String attr,
      String value, boolean finalParameter, String[] source) {
    if (value != null || allowNullValueProperties) {
      if (value == null) {
        value = DEFAULT_STRING_CHECK;
      }
      if (!finalParameters.contains(attr)) {
        properties.setProperty(attr, value);
        if(source != null) {
          updatingResource.put(attr, source);

hadoop-common-project/hadoop-common/src/test/java/org/apache/hadoop/conf/TestConfiguration.java
  }

  public void testNullValueProperties() throws Exception {
    Configuration conf = new Configuration();
    conf.setAllowNullValueProperties(true);
    out = new BufferedWriter(new FileWriter(CONFIG));
    startConfig();
    appendProperty("attr", "value", true);
    appendProperty("attr", "", true);
    endConfig();
    Path fileResource = new Path(CONFIG);
    conf.addResource(fileResource);
    assertEquals("value", conf.get("attr"));
  }

  public static void main(String[] argv) throws Exception {
    junit.textui.TestRunner.main(new String[]{
      TestConfiguration.class.getName()

