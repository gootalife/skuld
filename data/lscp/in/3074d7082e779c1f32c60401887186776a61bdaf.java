hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-shuffle/src/test/java/org/apache/hadoop/mapred/TestShuffleHandler.java
    Configuration conf = new Configuration();
    conf.setInt(ShuffleHandler.SHUFFLE_PORT_CONFIG_KEY, 0);
    conf.setInt(ShuffleHandler.MAX_SHUFFLE_CONNECTIONS, 3);
    conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION,
        "simple");
    UserGroupInformation.setConfiguration(conf);
    File absLogDir = new File("target", TestShuffleHandler.class.
        getSimpleName() + "LocDir").getAbsoluteFile();
    conf.set(YarnConfiguration.NM_LOCAL_DIRS, absLogDir.getAbsolutePath());
      } catch (EOFException e) {
      }
      Assert.assertEquals("sendError called due to shuffle error",
          0, failures.size());
    } finally {
      shuffleHandler.stop();
      FileUtil.fullyDelete(absLogDir);

