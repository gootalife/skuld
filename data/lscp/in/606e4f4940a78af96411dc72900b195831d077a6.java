hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/tracing/TraceUtils.java
    return new HTraceConfiguration() {
      @Override
      public String get(String key) {
        return get(key, "");
      }

      @Override
      public String get(String key, String defaultValue) {
        String prefixedKey = prefix + key;
        if (extraMap.containsKey(prefixedKey)) {
          return extraMap.get(prefixedKey);
        }
        return conf.get(prefixedKey, defaultValue);
      }
    };
  }

hadoop-common-project/hadoop-common/src/test/java/org/apache/hadoop/tracing/TestTraceUtils.java
    conf.set(TEST_PREFIX + key, oldValue);
    LinkedList<ConfigurationPair> extraConfig =
        new LinkedList<ConfigurationPair>();
    extraConfig.add(new ConfigurationPair(TEST_PREFIX + key, newValue));
    HTraceConfiguration wrapped = TraceUtils.wrapHadoopConf(TEST_PREFIX, conf, extraConfig);
    assertEquals(newValue, wrapped.get(key));
  }

