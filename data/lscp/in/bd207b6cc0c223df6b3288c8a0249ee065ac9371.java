hadoop-common-project/hadoop-common/src/test/java/org/apache/hadoop/conf/TestConfigurationFieldsBase.java
  private Set<String> xmlFieldsMissingInConfiguration = null;

  protected boolean configDebug = false;
  protected boolean xmlDebug = false;

    HashMap<String,String> retVal = new HashMap<String,String>();

    String propRegex = "^[A-Za-z][A-Za-z0-9_-]+(\\.[A-Za-z0-9_-]+)+$";
    Pattern p = Pattern.compile(propRegex);

    int totalFields = 0;
    String value;
    for (Field f : fields) {
      if (configDebug) {
        System.out.println("Field: " + f);
      }
      if (!Modifier.isStatic(f.getModifiers()) ||
          !Modifier.isPublic(f.getModifiers()) ||
      } catch (IllegalAccessException iaException) {
        continue;
      }
      if (configDebug) {
        System.out.println("  Value: " + value);
      }
      if (value.endsWith(".xml") ||
      Matcher m = p.matcher(value);
      if (!m.find()) {
        if (configDebug) {
          System.out.println("  Passes Regex: false");
        }
        continue;
      }
      if (configDebug) {
        System.out.println("  Passes Regex: true");
      }

      if (!retVal.containsKey(value)) {
        retVal.put(value,f.getName());
      } else {
        if (configDebug) {
          System.out.println("ERROR: Already found key for property " + value);
        }
      }
    }

    return retVal;
      if (xmlPropsToSkipCompare != null) {
        if (xmlPropsToSkipCompare.contains(key)) {
          if (xmlDebug) {
            System.out.println("  Skipping Full Key: " + key);
          }
          continue;
        }
      }
	}
      }
      if (skipPrefix) {
        if (xmlDebug) {
          System.out.println("  Skipping Prefix Key: " + key);
        }
        continue;
      }
      if (conf.onlyKeyExists(key)) {
        retVal.put(key,null);
        if (xmlDebug) {
          System.out.println("  XML Key,Null Value: " + key);
        }
      } else {
        String value = conf.get(key);
        if (value!=null) {
          retVal.put(key,entry.getValue());
          if (xmlDebug) {
            System.out.println("  XML Key,Valid Value: " + key);
          }
        }
      }
      kvItr.remove();

    configurationMemberVariables = new HashMap<String,String>();
    if (configDebug) {
      System.out.println("Reading configuration classes");
      System.out.println("");
    }
    for (Class c : configurationClasses) {
      Field[] fields = c.getDeclaredFields();
      Map<String,String> memberMap =
        configurationMemberVariables.putAll(memberMap);
      }
    }
    if (configDebug) {
      System.out.println("");
      System.out.println("=====");
      System.out.println("");
    }

    if (xmlDebug) {
      System.out.println("Reading XML property files");
      System.out.println("");
    }
    xmlKeyValueMap = extractPropertiesFromXml(xmlFilename);
    if (xmlDebug) {
      System.out.println("");
      System.out.println("=====");
      System.out.println("");
    }

    configurationFieldsMissingInXmlFile = compareConfigurationToXmlFields

hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-app/src/test/java/org/apache/hadoop/mapred/TestMapreduceConfigFields.java
