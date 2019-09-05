hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/metrics2/impl/MetricsSystemImpl.java
          public void run() {
            try {
              onTimerEvent();
            } catch (Exception e) {
              LOG.warn("Error invoking metrics timer", e);
            }
          }
        }, millis, millis);

