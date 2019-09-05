hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/metrics2/impl/MetricsSourceAdapter.java
        if (lastRecs == null) {
          getAllMetrics = true;
        }
      } else {
        return;
      }

      if (getAllMetrics) {
        MetricsCollectorImpl builder = new MetricsCollectorImpl();
        getMetrics(builder, true);
      }

      updateAttrCache();
      if (getAllMetrics) {
        updateInfoCache();

