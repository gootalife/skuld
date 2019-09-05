hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/DirectoryCollection.java
public class DirectoryCollection {
  private static final Log LOG = LogFactory.getLog(DirectoryCollection.class);

  public enum DiskErrorCause {
    DISK_FULL, OTHER
  }
    }
  }

  public interface DirsChangeListener {
    void onDirsChanged();
  }


  private int goodDirsDiskUtilizationPercentage;

  private Set<DirsChangeListener> dirsChangeListeners;

                : utilizationPercentageCutOff);
    diskUtilizationSpaceCutoff =
        utilizationSpaceCutOff < 0 ? 0 : utilizationSpaceCutOff;

    dirsChangeListeners = new HashSet<DirsChangeListener>();
  }

  synchronized void registerDirsChangeListener(
      DirsChangeListener listener) {
    if (dirsChangeListeners.add(listener)) {
      listener.onDirsChanged();
    }
  }

  synchronized void deregisterDirsChangeListener(
      DirsChangeListener listener) {
    dirsChangeListeners.remove(listener);
  }

      }
    }
    setGoodDirsDiskUtilizationPercentage();
    if (setChanged) {
      for (DirsChangeListener listener : dirsChangeListeners) {
        listener.onDirsChanged();
      }
    }
    return setChanged;
  }


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/LocalDirsHandlerService.java
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.nodemanager.DirectoryCollection.DirsChangeListener;
import org.apache.hadoop.yarn.server.nodemanager.metrics.NodeManagerMetrics;

    super.serviceStop();
  }

  public void registerLocalDirsChangeListener(DirsChangeListener listener) {
    localDirs.registerDirsChangeListener(listener);
  }

  public void registerLogDirsChangeListener(DirsChangeListener listener) {
    logDirs.registerDirsChangeListener(listener);
  }

  public void deregisterLocalDirsChangeListener(DirsChangeListener listener) {
    localDirs.deregisterDirsChangeListener(listener);
  }

  public void deregisterLogDirsChangeListener(DirsChangeListener listener) {
    logDirs.deregisterDirsChangeListener(listener);
  }


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/localizer/ResourceLocalizationService.java
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.DeletionService;
import org.apache.hadoop.yarn.server.nodemanager.DeletionService.FileDeletionTask;
import org.apache.hadoop.yarn.server.nodemanager.DirectoryCollection.DirsChangeListener;
import org.apache.hadoop.yarn.server.nodemanager.LocalDirsHandlerService;
import org.apache.hadoop.yarn.server.nodemanager.api.LocalizationProtocol;
import org.apache.hadoop.yarn.server.nodemanager.api.ResourceLocalizationSpec;
  private LocalResourcesTracker publicRsrc;

  private LocalDirsHandlerService dirsHandler;
  private DirsChangeListener localDirsChangeListener;
  private DirsChangeListener logDirsChangeListener;
  private Context nmContext;

    localizerTracker = createLocalizerTracker(conf);
    addService(localizerTracker);
    dispatcher.register(LocalizerEventType.class, localizerTracker);
    localDirsChangeListener = new DirsChangeListener() {
      @Override
      public void onDirsChanged() {
        checkAndInitializeLocalDirs();
      }
    };
    logDirsChangeListener = new DirsChangeListener() {
      @Override
      public void onDirsChanged() {
        initializeLogDirs(lfs);
      }
    };
    super.serviceInit(conf);
  }

                                      server.getListenerAddress());
    LOG.info("Localizer started on port " + server.getPort());
    super.serviceStart();
    dirsHandler.registerLocalDirsChangeListener(localDirsChangeListener);
    dirsHandler.registerLogDirsChangeListener(logDirsChangeListener);
  }

  LocalizerTracker createLocalizerTracker(Configuration conf) {

  @Override
  public void serviceStop() throws Exception {
    dirsHandler.deregisterLocalDirsChangeListener(localDirsChangeListener);
    dirsHandler.deregisterLogDirsChangeListener(logDirsChangeListener);
    if (server != null) {
      server.stop();
    }
              DiskChecker.checkDir(new File(publicDirDestPath.toUri().getPath()));
            }

            synchronized (pending) {
        writeCredentials(nmPrivateCTokensPath);
        if (dirsHandler.areDisksHealthy()) {
          exec.startLocalizer(nmPrivateCTokensPath, localizationServerAddress,
              context.getUser(),
  }
  
  @VisibleForTesting
  void checkAndInitializeLocalDirs() {
    List<String> dirs = dirsHandler.getLocalDirs();
    List<String> checkFailedDirs = new ArrayList<String>();
    for (String dir : dirs) {
        throw new YarnRuntimeException(msg, e);
      }
    }
  }

  private boolean checkLocalDir(String localDir) {
    localDirPathFsPermissionsMap.put(sysDir, nmPrivatePermission);
    return localDirPathFsPermissionsMap;
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/test/java/org/apache/hadoop/yarn/server/nodemanager/TestDirectoryCollection.java
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.DirectoryCollection.DirsChangeListener;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
    Assert.assertEquals(100.0F, dc.getDiskUtilizationPercentageCutoff(), delta);
    Assert.assertEquals(0, dc.getDiskUtilizationSpaceCutoff());
  }

  @Test
  public void testDirsChangeListener() {
    DirsChangeListenerTest listener1 = new DirsChangeListenerTest();
    DirsChangeListenerTest listener2 = new DirsChangeListenerTest();
    DirsChangeListenerTest listener3 = new DirsChangeListenerTest();

    String dirA = new File(testDir, "dirA").getPath();
    String[] dirs = { dirA };
    DirectoryCollection dc = new DirectoryCollection(dirs, 0.0F);
    Assert.assertEquals(1, dc.getGoodDirs().size());
    Assert.assertEquals(listener1.num, 0);
    Assert.assertEquals(listener2.num, 0);
    Assert.assertEquals(listener3.num, 0);
    dc.registerDirsChangeListener(listener1);
    dc.registerDirsChangeListener(listener2);
    dc.registerDirsChangeListener(listener3);
    Assert.assertEquals(listener1.num, 1);
    Assert.assertEquals(listener2.num, 1);
    Assert.assertEquals(listener3.num, 1);

    dc.deregisterDirsChangeListener(listener3);
    dc.checkDirs();
    Assert.assertEquals(0, dc.getGoodDirs().size());
    Assert.assertEquals(listener1.num, 2);
    Assert.assertEquals(listener2.num, 2);
    Assert.assertEquals(listener3.num, 1);

    dc.deregisterDirsChangeListener(listener2);
    dc.setDiskUtilizationPercentageCutoff(100.0F);
    dc.checkDirs();
    Assert.assertEquals(1, dc.getGoodDirs().size());
    Assert.assertEquals(listener1.num, 3);
    Assert.assertEquals(listener2.num, 2);
    Assert.assertEquals(listener3.num, 1);
  }

  static class DirsChangeListenerTest implements DirsChangeListener {
    public int num = 0;
    public DirsChangeListenerTest() {
    }
    @Override
    public void onDirsChanged() {
      num++;
    }
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/test/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/localizer/TestResourceLocalizationService.java
          isA(Configuration.class));

      spyService.init(conf);

      final FsPermission defaultPerm = new FsPermission((short)0755);

            .mkdir(eq(publicCache),eq(defaultPerm), eq(true));
      }

      spyService.start();

      final String user = "user0";
      final Application app = mock(Application.class);
      r.setSeed(seed);

      final LocalResource pubResource1 = getPublicMockedResource(r);
      final LocalResourceRequest pubReq1 =
          new LocalResourceRequest(pubResource1);

      LocalResource pubResource2 = null;
      do {
        pubResource2 = getPublicMockedResource(r);
      } while (pubResource2 == null || pubResource2.equals(pubResource1));
      final LocalResourceRequest pubReq2 =
          new LocalResourceRequest(pubResource2);

      Set<LocalResourceRequest> pubRsrcs = new HashSet<LocalResourceRequest>();
      pubRsrcs.add(pubReq1);
      pubRsrcs.add(pubReq2);

      Map<LocalResourceVisibility, Collection<LocalResourceRequest>> req =
          new HashMap<LocalResourceVisibility,
              Collection<LocalResourceRequest>>();
      req.put(LocalResourceVisibility.PUBLIC, pubRsrcs);

      spyService.handle(new ContainerLocalizationRequestEvent(c, req));
      dispatcher.await();

      verify(spyService, times(1)).checkAndInitializeLocalDirs();

      for (Path p : localDirs) {
        p = new Path((new URI(p.toString())).getPath());

