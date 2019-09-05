hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/test/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/localizer/MockLocalizerStatus.java
      return false;
    }
    MockLocalizerStatus other = (MockLocalizerStatus) o;
    return getLocalizerId().equals(other.getLocalizerId())
      && getResources().containsAll(other.getResources())
      && other.getResources().containsAll(getResources());
  }

