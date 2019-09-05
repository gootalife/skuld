hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/recovery/FileSystemRMStateStore.java
import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.hadoop.security.token.delegation.DelegationKey;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.proto.YarnServerCommonProtos.VersionProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerRecoveryProtos.AMRMTokenSecretManagerStateProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerRecoveryProtos.ApplicationAttemptStateDataProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerRecoveryProtos.ApplicationStateDataProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerRecoveryProtos.EpochProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerRecoveryProtos.ReservationAllocationStateProto;
import org.apache.hadoop.yarn.security.client.RMDelegationTokenIdentifier;
import org.apache.hadoop.yarn.server.records.Version;
import org.apache.hadoop.yarn.server.records.impl.pb.VersionPBImpl;
public class FileSystemRMStateStore extends RMStateStore {


  protected static final String ROOT_DIR_NAME = "FSRMStateRoot";
  protected static final Version CURRENT_VERSION_INFO = Version
    .newInstance(1, 3);
  protected static final String AMRMTOKEN_SECRET_MANAGER_NODE =
      "AMRMTokenSecretManagerNode";

  Path fsWorkingPath;

  Path amrmTokenSecretManagerRoot;
  private Path reservationRoot;

  @Override
  public synchronized void initInternal(Configuration conf)
      throws Exception{
    rmAppRoot = new Path(rootDirPath, RM_APP_ROOT);
    amrmTokenSecretManagerRoot =
        new Path(rootDirPath, AMRMTOKEN_SECRET_MANAGER_ROOT);
    reservationRoot = new Path(rootDirPath, RESERVATION_SYSTEM_ROOT);
    fsNumRetries =
        conf.getInt(YarnConfiguration.FS_RM_STATE_STORE_NUM_RETRIES,
            YarnConfiguration.DEFAULT_FS_RM_STATE_STORE_NUM_RETRIES);
    mkdirsWithRetries(rmDTSecretManagerRoot);
    mkdirsWithRetries(rmAppRoot);
    mkdirsWithRetries(amrmTokenSecretManagerRoot);
    mkdirsWithRetries(reservationRoot);
  }

  @Override
    loadRMAppState(rmState);
    loadAMRMTokenSecretManagerState(rmState);
    loadReservationSystemState(rmState);
    return rmState;
  }

  private void loadReservationSystemState(RMState rmState) throws Exception {
    try {
      final ReservationStateFileProcessor fileProcessor = new
          ReservationStateFileProcessor(rmState);
      final Path rootDirectory = this.reservationRoot;

      processDirectoriesOfFiles(fileProcessor, rootDirectory);
    } catch (Exception e) {
      LOG.error("Failed to load state.", e);
      throw e;
    }
  }

  private void loadAMRMTokenSecretManagerState(RMState rmState)
      throws Exception {
    checkAndResumeUpdateOperation(amrmTokenSecretManagerRoot);

  private void loadRMAppState(RMState rmState) throws Exception {
    try {
      List<ApplicationAttemptStateData> attempts = new ArrayList<>();
      final RMAppStateFileProcessor rmAppStateFileProcessor =
          new RMAppStateFileProcessor(rmState, attempts);
      final Path rootDirectory = this.rmAppRoot;

      processDirectoriesOfFiles(rmAppStateFileProcessor, rootDirectory);

    }
  }

  private void processDirectoriesOfFiles(
      RMStateFileProcessor rmAppStateFileProcessor, Path rootDirectory)
    throws Exception {
    for (FileStatus dir : listStatusWithRetries(rootDirectory)) {
      checkAndResumeUpdateOperation(dir.getPath());
      String dirName = dir.getPath().getName();
      for (FileStatus fileNodeStatus : listStatusWithRetries(dir.getPath())) {
        assert fileNodeStatus.isFile();
        String fileName = fileNodeStatus.getPath().getName();
        if (checkAndRemovePartialRecordWithRetries(fileNodeStatus.getPath())) {
          continue;
        }
        byte[] fileData = readFileWithRetries(fileNodeStatus.getPath(),
                fileNodeStatus.getLen());
        setUnreadableBySuperuserXattrib(fileNodeStatus.getPath());

        rmAppStateFileProcessor.processChildNode(dirName, fileName,
            fileData);
      }
    }
  }

  private boolean checkAndRemovePartialRecord(Path record) throws IOException {
    }
  }

  @Override
  protected void storeReservationState(
      ReservationAllocationStateProto reservationAllocation, String planName,
      String reservationIdName) throws Exception {
    Path planCreatePath = getNodePath(reservationRoot, planName);
    mkdirsWithRetries(planCreatePath);
    Path reservationPath = getNodePath(planCreatePath, reservationIdName);
    LOG.info("Storing state for reservation " + reservationIdName + " from " +
        "plan " + planName + " at path " + reservationPath);
    byte[] reservationData = reservationAllocation.toByteArray();
    writeFileWithRetries(reservationPath, reservationData, true);
  }

  @Override
  protected void updateReservationState(
      ReservationAllocationStateProto reservationAllocation, String planName,
      String reservationIdName) throws Exception {
    Path planCreatePath = getNodePath(reservationRoot, planName);
    Path reservationPath = getNodePath(planCreatePath, reservationIdName);
    LOG.info("Updating state for reservation " + reservationIdName + " from " +
        "plan " + planName + " at path " + reservationPath);
    byte[] reservationData = reservationAllocation.toByteArray();
    updateFile(reservationPath, reservationData, true);
  }

  @Override
  protected void removeReservationState(
      String planName, String reservationIdName) throws Exception {
    Path planCreatePath = getNodePath(reservationRoot, planName);
    Path reservationPath = getNodePath(planCreatePath, reservationIdName);
    LOG.info("Removing state for reservation " + reservationIdName + " from " +
        "plan " + planName + " at path " + reservationPath);
    deleteFileWithRetries(reservationPath);
  }

  @VisibleForTesting
  public int getNumRetries() {
    return fsNumRetries;
    return fsRetryInterval;
  }

  private void setUnreadableBySuperuserXattrib(Path p) throws IOException {
    if (fs.getScheme().toLowerCase().contains("hdfs")
        && intermediateEncryptionEnabled
        && !fs.getXAttrs(p).containsKey(UNREADABLE_BY_SUPERUSER_XATTRIB)) {
        EnumSet.of(XAttrSetFlag.CREATE));
    }
  }

  private static class ReservationStateFileProcessor implements
      RMStateFileProcessor {
    private RMState rmState;
    public ReservationStateFileProcessor(RMState state) {
      this.rmState = state;
    }

    @Override
    public void processChildNode(String planName, String childNodeName,
        byte[] childData) throws IOException {
      ReservationAllocationStateProto allocationState =
          ReservationAllocationStateProto.parseFrom(childData);
      if (!rmState.getReservationState().containsKey(planName)) {
        rmState.getReservationState().put(planName,
            new HashMap<ReservationId, ReservationAllocationStateProto>());
      }
      ReservationId reservationId =
          ReservationId.parseReservationId(childNodeName);
      rmState.getReservationState().get(planName).put(reservationId,
          allocationState);
    }
  }

  private static class RMAppStateFileProcessor implements RMStateFileProcessor {
    private RMState rmState;
    private List<ApplicationAttemptStateData> attempts;

    public RMAppStateFileProcessor(RMState rmState,
        List<ApplicationAttemptStateData> attempts) {
      this.rmState = rmState;
      this.attempts = attempts;
    }

    @Override
    public void processChildNode(String appDirName, String childNodeName,
        byte[] childData)
        throws com.google.protobuf.InvalidProtocolBufferException {
      if (childNodeName.startsWith(ApplicationId.appIdStrPrefix)) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Loading application from node: " + childNodeName);
        }
        ApplicationStateDataPBImpl appState =
            new ApplicationStateDataPBImpl(
                ApplicationStateDataProto.parseFrom(childData));
        ApplicationId appId =
            appState.getApplicationSubmissionContext().getApplicationId();
        rmState.appState.put(appId, appState);
      } else if (childNodeName.startsWith(
          ApplicationAttemptId.appAttemptIdStrPrefix)) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Loading application attempt from node: "
              + childNodeName);
        }
        ApplicationAttemptStateDataPBImpl attemptState =
            new ApplicationAttemptStateDataPBImpl(
                ApplicationAttemptStateDataProto.parseFrom(childData));
        attempts.add(attemptState);
      } else {
        LOG.info("Unknown child node with name: " + childNodeName);
      }
    }
  }

  private interface RMStateFileProcessor {
    void processChildNode(String appDirName, String childNodeName,
        byte[] childData)
        throws IOException;
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/recovery/LeveldbRMStateStore.java
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.hadoop.security.token.delegation.DelegationKey;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.proto.YarnServerCommonProtos.VersionProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerRecoveryProtos.EpochProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerRecoveryProtos.ApplicationAttemptStateDataProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerRecoveryProtos.ApplicationStateDataProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerRecoveryProtos.ReservationAllocationStateProto;
import org.apache.hadoop.yarn.security.client.RMDelegationTokenIdentifier;
import org.apache.hadoop.yarn.server.records.Version;
import org.apache.hadoop.yarn.server.records.impl.pb.VersionPBImpl;

import com.google.common.annotations.VisibleForTesting;

public class LeveldbRMStateStore extends RMStateStore {

  public static final Log LOG =
      RM_DT_SECRET_MANAGER_ROOT + SEPARATOR + "RMDTSequentialNumber";
  private static final String RM_APP_KEY_PREFIX =
      RM_APP_ROOT + SEPARATOR + ApplicationId.appIdStrPrefix;
  private static final String RM_RESERVATION_KEY_PREFIX =
      RESERVATION_SYSTEM_ROOT + SEPARATOR;

  private static final Version CURRENT_VERSION_INFO = Version
      .newInstance(1, 1);

  private DB db;

    return RM_DT_TOKEN_KEY_PREFIX + tokenId.getSequenceNumber();
  }

  private String getReservationNodeKey(String planName,
      String reservationId) {
    return RESERVATION_SYSTEM_ROOT + SEPARATOR + planName + SEPARATOR
        + reservationId;
  }

  @Override
  protected void initInternal(Configuration conf) throws Exception {
  }
     loadRMDTSecretManagerState(rmState);
     loadRMApps(rmState);
     loadAMRMTokenSecretManagerState(rmState);
    loadReservationState(rmState);
    return rmState;
   }

  private void loadReservationState(RMState rmState) throws IOException {
    int numReservations = 0;
    LeveldbIterator iter = null;
    try {
      iter = new LeveldbIterator(db);
      iter.seek(bytes(RM_RESERVATION_KEY_PREFIX));
      while (iter.hasNext()) {
        Entry<byte[],byte[]> entry = iter.next();
        String key = asString(entry.getKey());

        String planReservationString =
            key.substring(RM_RESERVATION_KEY_PREFIX.length());
        String[] parts = planReservationString.split(SEPARATOR);
        if (parts.length != 2) {
          LOG.warn("Incorrect reservation state key " + key);
          continue;
        }
        String planName = parts[0];
        String reservationName = parts[1];
        ReservationAllocationStateProto allocationState =
            ReservationAllocationStateProto.parseFrom(entry.getValue());
        if (!rmState.getReservationState().containsKey(planName)) {
          rmState.getReservationState().put(planName,
              new HashMap<ReservationId, ReservationAllocationStateProto>());
        }
        ReservationId reservationId =
            ReservationId.parseReservationId(reservationName);
        rmState.getReservationState().get(planName).put(reservationId,
            allocationState);
        numReservations++;
      }
    } catch (DBException e) {
      throw new IOException(e);
    } finally {
      if (iter != null) {
        iter.close();
      }
    }
    LOG.info("Recovered " + numReservations + " reservations");
  }

  private void loadRMDTSecretManagerState(RMState state) throws IOException {
    int numKeys = loadRMDTSecretManagerKeys(state);
    LOG.info("Recovered " + numKeys + " RM delegation token master keys");
    }
  }

  @Override
  protected void storeReservationState(
      ReservationAllocationStateProto reservationAllocation, String planName,
      String reservationIdName) throws Exception {
    try {
      WriteBatch batch = db.createWriteBatch();
      try {
        String key = getReservationNodeKey(planName, reservationIdName);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Storing state for reservation " + reservationIdName
              + " plan " + planName + " at " + key);
        }
        batch.put(bytes(key), reservationAllocation.toByteArray());
        db.write(batch);
      } finally {
        batch.close();
      }
    } catch (DBException e) {
      throw new IOException(e);
    }
  }

  @Override
  protected void updateReservationState(
      ReservationAllocationStateProto reservationAllocation, String planName,
      String reservationIdName) throws Exception {
    storeReservationState(reservationAllocation, planName,
        reservationIdName);
  }

  @Override
  protected void removeReservationState(String planName,
      String reservationIdName) throws Exception {
    try {
      WriteBatch batch = db.createWriteBatch();
      try {
        String reservationKey =
            getReservationNodeKey(planName, reservationIdName);
        batch.delete(bytes(reservationKey));
        if (LOG.isDebugEnabled()) {
          LOG.debug("Removing state for reservation " + reservationIdName
              + " plan " + planName + " at " + reservationKey);
        }
        db.write(batch);
      } finally {
        batch.close();
      }
    } catch (DBException e) {
      throw new IOException(e);
    }
  }

  private void storeOrUpdateRMDT(RMDelegationTokenIdentifier tokenId,
      Long renewDate, boolean isUpdate) throws IOException {
    String tokenKey = getRMDTTokenNodeKey(tokenId);

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/recovery/MemoryRMStateStore.java
package org.apache.hadoop.yarn.server.resourcemanager.recovery;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.security.token.delegation.DelegationKey;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerRecoveryProtos.ReservationAllocationStateProto;
import org.apache.hadoop.yarn.security.client.RMDelegationTokenIdentifier;
import org.apache.hadoop.yarn.server.records.Version;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.AMRMTokenSecretManagerState;
    rmDTMasterKeyState.remove(delegationKey);
  }

  @Override
  protected synchronized void storeReservationState(
      ReservationAllocationStateProto reservationAllocation, String planName,
      String reservationIdName) throws Exception {
    LOG.info("Storing reservationallocation for " + reservationIdName + " " +
            "for plan " + planName);
    Map<ReservationId, ReservationAllocationStateProto> planState =
        state.getReservationState().get(planName);
    if (planState == null) {
      planState = new HashMap<>();
      state.getReservationState().put(planName, planState);
    }
    ReservationId reservationId =
        ReservationId.parseReservationId(reservationIdName);
    planState.put(reservationId, reservationAllocation);
  }

  @Override
  protected synchronized void updateReservationState(
      ReservationAllocationStateProto reservationAllocation, String planName,
      String reservationIdName) throws Exception {
    LOG.info("Updating reservationallocation for " + reservationIdName + " " +
            "for plan " + planName);
    Map<ReservationId, ReservationAllocationStateProto> planState =
        state.getReservationState().get(planName);
    if (planState == null) {
      throw new YarnRuntimeException("State for plan " + planName + " does " +
          "not exist");
    }
    ReservationId reservationId =
        ReservationId.parseReservationId(reservationIdName);
    planState.put(reservationId, reservationAllocation);
  }

  @Override
  protected synchronized void removeReservationState(
      String planName, String reservationIdName) throws Exception {
    LOG.info("Removing reservationallocation " + reservationIdName
              + " for plan " + planName);

    Map<ReservationId, ReservationAllocationStateProto> planState =
        state.getReservationState().get(planName);
    if (planState == null) {
      throw new YarnRuntimeException("State for plan " + planName + " does " +
          "not exist");
    }
    ReservationId reservationId =
        ReservationId.parseReservationId(reservationIdName);
    planState.remove(reservationId);
    if (planState.isEmpty()) {
      state.getReservationState().remove(planName);
    }
  }

  @Override
  protected Version loadVersion() throws Exception {
    return null;

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/recovery/NullRMStateStore.java
import org.apache.hadoop.security.token.delegation.DelegationKey;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerRecoveryProtos.ReservationAllocationStateProto;
import org.apache.hadoop.yarn.security.client.RMDelegationTokenIdentifier;
import org.apache.hadoop.yarn.server.records.Version;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.AMRMTokenSecretManagerState;
  }

  @Override
  protected void storeReservationState(
      ReservationAllocationStateProto reservationAllocation, String planName,
      String reservationIdName) throws Exception {
  }

  @Override
  protected void removeReservationState(String planName,
      String reservationIdName) throws Exception {
  }

  @Override
  protected void updateReservationState(
      ReservationAllocationStateProto reservationAllocation, String planName,
      String reservationIdName) throws Exception {
  }

  @Override
  public void removeRMDTMasterKeyState(DelegationKey delegationKey) throws Exception {

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/recovery/RMStateStore.java
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationSubmissionContextPBImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerRecoveryProtos.ReservationAllocationStateProto;
import org.apache.hadoop.yarn.security.client.RMDelegationTokenIdentifier;
import org.apache.hadoop.yarn.server.records.Version;
import org.apache.hadoop.yarn.server.resourcemanager.RMFatalEvent;
      "RMDTSequenceNumber_";
  protected static final String AMRMTOKEN_SECRET_MANAGER_ROOT =
      "AMRMTokenSecretManagerRoot";
  protected static final String RESERVATION_SYSTEM_ROOT =
      "ReservationSystemRoot";
  protected static final String VERSION_NODE = "RMVersionNode";
  protected static final String EPOCH_NODE = "EpochNode";
  private ResourceManager resourceManager;
       .addTransition(RMStateStoreState.ACTIVE, RMStateStoreState.ACTIVE,
           RMStateStoreEventType.UPDATE_AMRM_TOKEN,
           new StoreOrUpdateAMRMTokenTransition())
      .addTransition(RMStateStoreState.ACTIVE, RMStateStoreState.ACTIVE,
          RMStateStoreEventType.STORE_RESERVATION,
          new StoreReservationAllocationTransition())
      .addTransition(RMStateStoreState.ACTIVE, RMStateStoreState.ACTIVE,
          RMStateStoreEventType.UPDATE_RESERVATION,
          new UpdateReservationAllocationTransition())
      .addTransition(RMStateStoreState.ACTIVE, RMStateStoreState.ACTIVE,
          RMStateStoreEventType.REMOVE_RESERVATION,
          new RemoveReservationAllocationTransition())
      .addTransition(RMStateStoreState.ACTIVE, RMStateStoreState.FENCED,
          RMStateStoreEventType.FENCED)
      .addTransition(RMStateStoreState.FENCED, RMStateStoreState.FENCED,
          RMStateStoreEventType.STORE_DELEGATION_TOKEN,
          RMStateStoreEventType.REMOVE_DELEGATION_TOKEN,
          RMStateStoreEventType.UPDATE_DELEGATION_TOKEN,
          RMStateStoreEventType.UPDATE_AMRM_TOKEN,
          RMStateStoreEventType.STORE_RESERVATION,
          RMStateStoreEventType.UPDATE_RESERVATION,
          RMStateStoreEventType.REMOVE_RESERVATION));

  private final StateMachine<RMStateStoreState,
                             RMStateStoreEventType,
    }
  }

  private static class StoreReservationAllocationTransition implements
      SingleArcTransition<RMStateStore, RMStateStoreEvent> {
    @Override
    public void transition(RMStateStore store, RMStateStoreEvent event) {
      if (!(event instanceof RMStateStoreStoreReservationEvent)) {
        LOG.error("Illegal event type: " + event.getClass());
        return;
      }
      RMStateStoreStoreReservationEvent reservationEvent =
          (RMStateStoreStoreReservationEvent) event;
      try {
        LOG.info("Storing reservation allocation." + reservationEvent
                .getReservationIdName());
        store.storeReservationState(
            reservationEvent.getReservationAllocation(),
            reservationEvent.getPlanName(),
            reservationEvent.getReservationIdName());
      } catch (Exception e) {
        LOG.error("Error while storing reservation allocation.", e);
        store.notifyStoreOperationFailed(e);
      }
    }
  }

  private static class UpdateReservationAllocationTransition implements
      SingleArcTransition<RMStateStore, RMStateStoreEvent> {
    @Override
    public void transition(RMStateStore store, RMStateStoreEvent event) {
      if (!(event instanceof RMStateStoreStoreReservationEvent)) {
        LOG.error("Illegal event type: " + event.getClass());
        return;
      }
      RMStateStoreStoreReservationEvent reservationEvent =
          (RMStateStoreStoreReservationEvent) event;
      try {
        LOG.info("Updating reservation allocation." + reservationEvent
                .getReservationIdName());
        store.updateReservationState(
            reservationEvent.getReservationAllocation(),
            reservationEvent.getPlanName(),
            reservationEvent.getReservationIdName());
      } catch (Exception e) {
        LOG.error("Error while updating reservation allocation.", e);
        store.notifyStoreOperationFailed(e);
      }
    }
  }

  private static class RemoveReservationAllocationTransition implements
      SingleArcTransition<RMStateStore, RMStateStoreEvent> {
    @Override
    public void transition(RMStateStore store, RMStateStoreEvent event) {
      if (!(event instanceof RMStateStoreStoreReservationEvent)) {
        LOG.error("Illegal event type: " + event.getClass());
        return;
      }
      RMStateStoreStoreReservationEvent reservationEvent =
          (RMStateStoreStoreReservationEvent) event;
      try {
        LOG.info("Removing reservation allocation." + reservationEvent
                .getReservationIdName());
        store.removeReservationState(
            reservationEvent.getPlanName(),
            reservationEvent.getReservationIdName());
      } catch (Exception e) {
        LOG.error("Error while removing reservation allocation.", e);
        store.notifyStoreOperationFailed(e);
      }
    }
  }

  public RMStateStore() {
    super(RMStateStore.class.getName());
    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    AMRMTokenSecretManagerState amrmTokenSecretManagerState = null;

    private Map<String, Map<ReservationId, ReservationAllocationStateProto>>
        reservationState = new TreeMap<>();

    public Map<ApplicationId, ApplicationStateData> getApplicationState() {
      return appState;
    }
    public AMRMTokenSecretManagerState getAMRMTokenSecretManagerState() {
      return amrmTokenSecretManagerState;
    }

    public Map<String, Map<ReservationId, ReservationAllocationStateProto>>
        getReservationState() {
      return reservationState;
    }
  }
    
  private Dispatcher rmDispatcher;
        RMStateStoreEventType.REMOVE_MASTERKEY));
  }

  public void storeNewReservation(
      ReservationAllocationStateProto reservationAllocation, String planName,
      String reservationIdName) {
    handleStoreEvent(new RMStateStoreStoreReservationEvent(
        reservationAllocation, RMStateStoreEventType.STORE_RESERVATION,
        planName, reservationIdName));
  }

  public void updateReservation(
      ReservationAllocationStateProto reservationAllocation,
      String planName, String reservationIdName) {
    handleStoreEvent(new RMStateStoreStoreReservationEvent(
        reservationAllocation, RMStateStoreEventType.UPDATE_RESERVATION,
        planName, reservationIdName));
  }

  public void removeReservation(String planName, String reservationIdName) {
    handleStoreEvent(new RMStateStoreStoreReservationEvent(
            null, RMStateStoreEventType.REMOVE_RESERVATION,
            planName, reservationIdName));
  }

  protected abstract void storeReservationState(
      ReservationAllocationStateProto reservationAllocation, String planName,
      String reservationIdName) throws Exception;

  protected abstract void removeReservationState(String planName,
      String reservationIdName) throws Exception;

  protected abstract void updateReservationState(
      ReservationAllocationStateProto reservationAllocation, String planName,
      String reservationIdName) throws Exception;


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/recovery/RMStateStoreEventType.java
  STORE_DELEGATION_TOKEN,
  REMOVE_DELEGATION_TOKEN,
  UPDATE_DELEGATION_TOKEN,
  UPDATE_AMRM_TOKEN,
  STORE_RESERVATION,
  UPDATE_RESERVATION,
  REMOVE_RESERVATION,
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/recovery/RMStateStoreStoreReservationEvent.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/recovery/RMStateStoreStoreReservationEvent.java

package org.apache.hadoop.yarn.server.resourcemanager.recovery;

import org.apache.hadoop.yarn.proto.YarnServerResourceManagerRecoveryProtos.ReservationAllocationStateProto;

public class RMStateStoreStoreReservationEvent extends RMStateStoreEvent {

  private ReservationAllocationStateProto reservationAllocation;
  private String planName;
  private String reservationIdName;

  public RMStateStoreStoreReservationEvent(RMStateStoreEventType type) {
    super(type);
  }

  public RMStateStoreStoreReservationEvent(
      ReservationAllocationStateProto reservationAllocationState,
      RMStateStoreEventType type, String planName, String reservationIdName) {
    this(type);
    this.reservationAllocation = reservationAllocationState;
    this.planName = planName;
    this.reservationIdName = reservationIdName;
  }

  public ReservationAllocationStateProto getReservationAllocation() {
    return reservationAllocation;
  }

  public String getPlanName() {
    return planName;
  }

  public String getReservationIdName() {
    return reservationIdName;
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/recovery/ZKRMStateStore.java
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.google.common.base.Preconditions;
import org.apache.hadoop.util.ZKUtil;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.conf.HAUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerRecoveryProtos.ApplicationAttemptStateDataProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerRecoveryProtos.ApplicationStateDataProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerRecoveryProtos.EpochProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerRecoveryProtos.ReservationAllocationStateProto;
import org.apache.hadoop.yarn.security.client.RMDelegationTokenIdentifier;
import org.apache.hadoop.yarn.server.records.Version;
import org.apache.hadoop.yarn.server.records.impl.pb.VersionPBImpl;
@Private
@Unstable

  protected static final String ROOT_ZNODE_NAME = "ZKRMStateRoot";
  protected static final Version CURRENT_VERSION_INFO = Version
      .newInstance(1, 3);
  private static final String RM_DELEGATION_TOKENS_ROOT_ZNODE_NAME =
      "RMDelegationTokensRoot";
  private static final String RM_DT_SEQUENTIAL_NUMBER_ZNODE_NAME =
  private String delegationTokensRootPath;
  private String dtSequenceNumberPath;
  private String amrmTokenSecretManagerRoot;
  private String reservationRoot;
  @VisibleForTesting
  protected String znodeWorkingPath;

        RM_DT_SEQUENTIAL_NUMBER_ZNODE_NAME);
    amrmTokenSecretManagerRoot =
        getNodePath(zkRootNodePath, AMRMTOKEN_SECRET_MANAGER_ROOT);
    reservationRoot = getNodePath(zkRootNodePath, RESERVATION_SYSTEM_ROOT);
  }

  @Override
    create(delegationTokensRootPath);
    create(dtSequenceNumberPath);
    create(amrmTokenSecretManagerRoot);
    create(reservationRoot);
  }

  private void logRootNodeAcls(String prefix) throws Exception {
    loadRMAppState(rmState);
    loadAMRMTokenSecretManagerState(rmState);
    loadReservationSystemState(rmState);
    return rmState;
  }

  private void loadReservationSystemState(RMState rmState) throws Exception {
    List<String> planNodes = getChildren(reservationRoot);
    for (String planName : planNodes) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Loading plan from znode: " + planName);
      }
      String planNodePath = getNodePath(reservationRoot, planName);

      List<String> reservationNodes = getChildren(planNodePath);
      for (String reservationNodeName : reservationNodes) {
        String reservationNodePath = getNodePath(planNodePath,
            reservationNodeName);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Loading reservation from znode: " + reservationNodePath);
        }
        byte[] reservationData = getData(reservationNodePath);
        ReservationAllocationStateProto allocationState =
            ReservationAllocationStateProto.parseFrom(reservationData);
        if (!rmState.getReservationState().containsKey(planName)) {
          rmState.getReservationState().put(planName,
              new HashMap<ReservationId, ReservationAllocationStateProto>());
        }
        ReservationId reservationId =
            ReservationId.parseReservationId(reservationNodeName);
        rmState.getReservationState().get(planName).put(reservationId,
            allocationState);
      }
    }
  }

  private void loadAMRMTokenSecretManagerState(RMState rmState)
      throws Exception {
    byte[] data = getData(amrmTokenSecretManagerRoot);
    safeSetData(amrmTokenSecretManagerRoot, stateData, -1);
  }

  @Override
  protected synchronized void removeReservationState(String planName,
      String reservationIdName)
      throws Exception {
    String planNodePath =
        getNodePath(reservationRoot, planName);
    String reservationPath = getNodePath(planNodePath,
        reservationIdName);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Removing reservationallocation " + reservationIdName + " for" +
          " plan " + planName);
    }
    safeDelete(reservationPath);

    List<String> reservationNodes = getChildren(planNodePath);
    if (reservationNodes.isEmpty()) {
      safeDelete(planNodePath);
    }
  }

  @Override
  protected synchronized void storeReservationState(
      ReservationAllocationStateProto reservationAllocation, String planName,
      String reservationIdName)
      throws Exception {
    SafeTransaction trx = new SafeTransaction();
    addOrUpdateReservationState(
        reservationAllocation, planName, reservationIdName, trx, false);
    trx.commit();
  }

  @Override
  protected synchronized void updateReservationState(
      ReservationAllocationStateProto reservationAllocation, String planName,
      String reservationIdName)
      throws Exception {
    SafeTransaction trx = new SafeTransaction();
    addOrUpdateReservationState(
        reservationAllocation, planName, reservationIdName, trx, true);
    trx.commit();
  }

  private void addOrUpdateReservationState(
      ReservationAllocationStateProto reservationAllocation, String planName,
      String reservationIdName, SafeTransaction trx, boolean isUpdate)
      throws Exception {
    String planCreatePath =
        getNodePath(reservationRoot, planName);
    String reservationPath = getNodePath(planCreatePath,
        reservationIdName);
    byte[] reservationData = reservationAllocation.toByteArray();

    if (!exists(planCreatePath)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Creating plan node: " + planName + " at: " + planCreatePath);
      }
      trx.create(planCreatePath, null, zkAcl, CreateMode.PERSISTENT);
    }

    if (isUpdate) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Updating reservation: " + reservationIdName + " in plan:"
            + planName + " at: " + reservationPath);
      }
      trx.setData(reservationPath, reservationData, -1);
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Storing reservation: " + reservationIdName + " in plan:"
            + planName + " at: " + reservationPath);
      }
      trx.create(reservationPath, reservationData, zkAcl,
          CreateMode.PERSISTENT);
    }
  }


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/reservation/ReservationSystemUtil.java

package org.apache.hadoop.yarn.server.resourcemanager.reservation;

import org.apache.hadoop.yarn.api.records.ReservationDefinition;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.ReservationRequest;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.impl.pb.ReservationDefinitionPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ReservationIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ResourcePBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos;
import org.apache.hadoop.yarn.proto.YarnProtos.ReservationDefinitionProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ReservationIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerRecoveryProtos.ReservationAllocationStateProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerRecoveryProtos.ResourceAllocationRequestProto;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

    }
    return resources;
  }

  public static ReservationAllocationStateProto buildStateProto(
      ReservationAllocation allocation) {
    ReservationAllocationStateProto.Builder builder =
        ReservationAllocationStateProto.newBuilder();

    builder.setAcceptanceTimestamp(allocation.getAcceptanceTime());
    builder.setContainsGangs(allocation.containsGangs());
    builder.setStartTime(allocation.getStartTime());
    builder.setEndTime(allocation.getEndTime());
    builder.setUser(allocation.getUser());
    ReservationDefinitionProto definitionProto = convertToProtoFormat(
        allocation.getReservationDefinition());
    builder.setReservationDefinition(definitionProto);

    for (Map.Entry<ReservationInterval, Resource> entry :
        allocation.getAllocationRequests().entrySet()) {
      ResourceAllocationRequestProto p =
          ResourceAllocationRequestProto.newBuilder()
          .setStartTime(entry.getKey().getStartTime())
          .setEndTime(entry.getKey().getEndTime())
          .setResource(convertToProtoFormat(entry.getValue()))
          .build();
      builder.addAllocationRequests(p);
    }

    ReservationAllocationStateProto allocationProto = builder.build();
    return allocationProto;
  }

  private static ReservationDefinitionProto convertToProtoFormat(
      ReservationDefinition reservationDefinition) {
    return ((ReservationDefinitionPBImpl)reservationDefinition).getProto();
  }

  public static ResourceProto convertToProtoFormat(Resource e) {
    return YarnProtos.ResourceProto.newBuilder()
        .setMemory(e.getMemory())
        .setVirtualCores(e.getVirtualCores())
        .build();
  }

  public static Map<ReservationInterval, Resource> toAllocations(
      List<ResourceAllocationRequestProto> allocationRequestsList) {
    Map<ReservationInterval, Resource> allocations = new HashMap<>();
    for (ResourceAllocationRequestProto proto : allocationRequestsList) {
      allocations.put(
          new ReservationInterval(proto.getStartTime(), proto.getEndTime()),
          convertFromProtoFormat(proto.getResource()));
    }
    return allocations;
  }

  private static ResourcePBImpl convertFromProtoFormat(ResourceProto resource) {
    return new ResourcePBImpl(resource);
  }

  public static ReservationDefinitionPBImpl convertFromProtoFormat(
      ReservationDefinitionProto r) {
    return new ReservationDefinitionPBImpl(r);
  }

  public static ReservationIdPBImpl convertFromProtoFormat(
      ReservationIdProto r) {
    return new ReservationIdPBImpl(r);
  }

  public static ReservationId toReservationId(
      ReservationIdProto reservationId) {
    return new ReservationIdPBImpl(reservationId);
  }

  public static InMemoryReservationAllocation toInMemoryAllocation(
      String planName, ReservationId reservationId,
      ReservationAllocationStateProto allocationState, Resource minAlloc,
      ResourceCalculator planResourceCalculator) {
    ReservationDefinition definition =
        convertFromProtoFormat(
            allocationState.getReservationDefinition());
    Map<ReservationInterval, Resource> allocations = toAllocations(
            allocationState.getAllocationRequestsList());
    InMemoryReservationAllocation allocation =
        new InMemoryReservationAllocation(reservationId, definition,
        allocationState.getUser(), planName, allocationState.getStartTime(),
        allocationState.getEndTime(), allocations, planResourceCalculator,
        minAlloc, allocationState.getContainsGangs());
    return allocation;
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/recovery/RMStateStoreTestBase.java

package org.apache.hadoop.yarn.server.resourcemanager.recovery;

import org.apache.hadoop.yarn.api.records.ReservationDefinition;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.event.Event;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import javax.crypto.SecretKey;

import org.apache.hadoop.yarn.proto.YarnServerResourceManagerRecoveryProtos.ReservationAllocationStateProto;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationSystemTestUtil;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationSystemUtil;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.junit.Assert;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore.RMDTSecretManagerState;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore.RMState;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.AMRMTokenSecretManagerState;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.InMemoryReservationAllocation;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationAllocation;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.AggregateAppResourceUsage;

    store.close();
  }

  public void testReservationStateStore(
      RMStateStoreHelper stateStoreHelper) throws Exception {
    RMStateStore store = stateStoreHelper.getRMStateStore();
    TestDispatcher dispatcher = new TestDispatcher();
    store.setRMDispatcher(dispatcher);

    RMContext rmContext = mock(RMContext.class);
    when(rmContext.getStateStore()).thenReturn(store);

    long ts = System.currentTimeMillis();
    ReservationId r1 = ReservationId.newInstance(ts, 1);
    int start = 1;
    int[] alloc = { 10, 10, 10, 10, 10 };
    ResourceCalculator res = new DefaultResourceCalculator();
    Resource minAlloc = Resource.newInstance(1024, 1);
    boolean hasGang = true;
    String planName = "dedicated";
    ReservationDefinition rDef =
        ReservationSystemTestUtil.createSimpleReservationDefinition(
            start, start + alloc.length + 1, alloc.length);
    ReservationAllocation allocation = new InMemoryReservationAllocation(
        r1, rDef, "u3", planName, 0, 0 + alloc.length,
        ReservationSystemTestUtil.generateAllocation(0L, 1L, alloc), res,
        minAlloc, hasGang);
    ReservationAllocationStateProto allocationStateProto =
        ReservationSystemUtil.buildStateProto(allocation);
    assertAllocationStateEqual(allocation, allocationStateProto);

    store = stateStoreHelper.getRMStateStore();
    when(rmContext.getStateStore()).thenReturn(store);
    store.setRMDispatcher(dispatcher);
    RMState state = store.loadState();
    Map<String, Map<ReservationId, ReservationAllocationStateProto>>
      reservationState = state.getReservationState();
    Assert.assertNotNull(reservationState);

    String reservationIdName = r1.toString();
    rmContext.getStateStore().storeNewReservation(
        allocationStateProto,
        planName, reservationIdName);


    validateStoredReservation(
        stateStoreHelper, dispatcher, rmContext, r1, planName, allocation,
        allocationStateProto);

    alloc = new int[]{6, 6, 6};
    hasGang = false;
    allocation = new InMemoryReservationAllocation(
        r1, rDef, "u3", planName, 2, 2 + alloc.length,
        ReservationSystemTestUtil.generateAllocation(1L, 2L, alloc), res,
        minAlloc, hasGang);
    allocationStateProto =
        ReservationSystemUtil.buildStateProto(allocation);
    rmContext.getStateStore().updateReservation(
        allocationStateProto,
        planName, reservationIdName);

    validateStoredReservation(
        stateStoreHelper, dispatcher, rmContext, r1, planName, allocation,
        allocationStateProto);

    ReservationId r2 = ReservationId.newInstance(ts, 2);
    ReservationAllocation allocation2 = new InMemoryReservationAllocation(
        r2, rDef, "u3", planName, 0, 0 + alloc.length,
        ReservationSystemTestUtil.generateAllocation(0L, 1L, alloc), res,
        minAlloc, hasGang);
    ReservationAllocationStateProto allocationStateProto2 =
        ReservationSystemUtil.buildStateProto(allocation2);
    String reservationIdName2 = r2.toString();

    rmContext.getStateStore().storeNewReservation(
        allocationStateProto2,
        planName, reservationIdName2);
    rmContext.getStateStore().removeReservation(planName, reservationIdName);

    Map<ReservationId, ReservationAllocationStateProto> reservations;

    store = stateStoreHelper.getRMStateStore();
    when(rmContext.getStateStore()).thenReturn(store);
    store.setRMDispatcher(dispatcher);
    state = store.loadState();
    reservationState = state.getReservationState();
    Assert.assertNotNull(reservationState);
    reservations = reservationState.get(planName);
    Assert.assertNotNull(reservations);
    ReservationAllocationStateProto storedReservationAllocation =
        reservations.get(r1);
    Assert.assertNull("Removed reservation should not be available in store",
        storedReservationAllocation);

    storedReservationAllocation = reservations.get(r2);
    assertAllocationStateEqual(
        allocationStateProto2, storedReservationAllocation);
    assertAllocationStateEqual(allocation2, storedReservationAllocation);


    rmContext.getStateStore().removeReservation(planName, reservationIdName2);

    store = stateStoreHelper.getRMStateStore();
    when(rmContext.getStateStore()).thenReturn(store);
    store.setRMDispatcher(dispatcher);
    state = store.loadState();
    reservationState = state.getReservationState();
    Assert.assertNotNull(reservationState);
    reservations = reservationState.get(planName);
    Assert.assertNull(reservations);
  }

  private void validateStoredReservation(
      RMStateStoreHelper stateStoreHelper, TestDispatcher dispatcher,
      RMContext rmContext, ReservationId r1, String planName,
      ReservationAllocation allocation,
      ReservationAllocationStateProto allocationStateProto) throws Exception {
    RMStateStore store = stateStoreHelper.getRMStateStore();
    when(rmContext.getStateStore()).thenReturn(store);
    store.setRMDispatcher(dispatcher);
    RMState state = store.loadState();
    Map<String, Map<ReservationId, ReservationAllocationStateProto>>
        reservationState = state.getReservationState();
    Assert.assertNotNull(reservationState);
    Map<ReservationId, ReservationAllocationStateProto> reservations =
        reservationState.get(planName);
    Assert.assertNotNull(reservations);
    ReservationAllocationStateProto storedReservationAllocation =
        reservations.get(r1);
    Assert.assertNotNull(storedReservationAllocation);

    assertAllocationStateEqual(
        allocationStateProto, storedReservationAllocation);
    assertAllocationStateEqual(allocation, storedReservationAllocation);
  }

  void assertAllocationStateEqual(
      ReservationAllocationStateProto expected,
      ReservationAllocationStateProto actual) {

    Assert.assertEquals(
        expected.getAcceptanceTimestamp(), actual.getAcceptanceTimestamp());
    Assert.assertEquals(expected.getStartTime(), actual.getStartTime());
    Assert.assertEquals(expected.getEndTime(), actual.getEndTime());
    Assert.assertEquals(expected.getContainsGangs(), actual.getContainsGangs());
    Assert.assertEquals(expected.getUser(), actual.getUser());
    assertEquals(
        expected.getReservationDefinition(), actual.getReservationDefinition());
    assertEquals(expected.getAllocationRequestsList(),
        actual.getAllocationRequestsList());
  }

  void assertAllocationStateEqual(
      ReservationAllocation expected,
      ReservationAllocationStateProto actual) {
    Assert.assertEquals(
        expected.getAcceptanceTime(), actual.getAcceptanceTimestamp());
    Assert.assertEquals(expected.getStartTime(), actual.getStartTime());
    Assert.assertEquals(expected.getEndTime(), actual.getEndTime());
    Assert.assertEquals(expected.containsGangs(), actual.getContainsGangs());
    Assert.assertEquals(expected.getUser(), actual.getUser());
    assertEquals(
        expected.getReservationDefinition(),
        ReservationSystemUtil.convertFromProtoFormat(
            actual.getReservationDefinition()));
    assertEquals(
        expected.getAllocationRequests(),
        ReservationSystemUtil.toAllocations(
            actual.getAllocationRequestsList()));
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/recovery/TestFSRMStateStore.java
      testDeleteStore(fsTester);
      testRemoveApplication(fsTester);
      testAMRMTokenSecretManagerStateStore(fsTester);
      testReservationStateStore(fsTester);
    } finally {
      cluster.shutdown();
    }

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/recovery/TestLeveldbRMStateStore.java
    testAMRMTokenSecretManagerStateStore(tester);
  }

  @Test(timeout = 60000)
  public void testReservation() throws Exception {
    LeveldbStateStoreTester tester = new LeveldbStateStoreTester();
    testReservationStateStore(tester);
  }

  class LeveldbStateStoreTester implements RMStateStoreHelper {

    @Override

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/recovery/TestZKRMStateStore.java
    testDeleteStore(zkTester);
    testRemoveApplication(zkTester);
    testAMRMTokenSecretManagerStateStore(zkTester);
    testReservationStateStore(zkTester);
    ((TestZKRMStateStoreTester.TestZKRMStateStoreInternal)
        zkTester.getRMStateStore()).testRetryingCreateRootDir();
  }

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/reservation/ReservationSystemTestUtil.java
    return scheduler;
  }

  public static ReservationDefinition createSimpleReservationDefinition(
      long arrival, long deadline, long duration) {
    ReservationRequest r =
        ReservationRequest.newInstance(Resource.newInstance(1024, 1), 1, 1,
            duration);
    ReservationDefinition rDef = new ReservationDefinitionPBImpl();
    ReservationRequests reqs = new ReservationRequestsPBImpl();
    reqs.setReservationResources(Collections.singletonList(r));
    reqs.setInterpreter(ReservationRequestInterpreter.R_ALL);
    rDef.setReservationRequests(reqs);
    rDef.setArrival(arrival);
    rDef.setDeadline(deadline);
    return rDef;
  }

  @SuppressWarnings("unchecked")
  public CapacityScheduler mockCapacityScheduler(int numContainers)
      throws IOException {

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/reservation/TestInMemoryReservationAllocation.java
package org.apache.hadoop.yarn.server.resourcemanager.reservation;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import org.apache.hadoop.yarn.api.records.ReservationDefinition;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.ReservationRequest;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.junit.After;
    int[] alloc = { 10, 10, 10, 10, 10, 10 };
    int start = 100;
    ReservationDefinition rDef =
        ReservationSystemTestUtil.createSimpleReservationDefinition(
            start, start + alloc.length + 1,
            alloc.length);
    Map<ReservationInterval, Resource> allocations =
        generateAllocation(start, alloc, false, false);
    int[] alloc = { 10, 10, 10, 10, 10, 10 };
    int start = 100;
    ReservationDefinition rDef =
        ReservationSystemTestUtil.createSimpleReservationDefinition(
            start, start + alloc.length + 1,
            alloc.length);
    Map<ReservationInterval, Resource> allocations =
        generateAllocation(start, alloc, true, false);
    int[] alloc = { 0, 5, 10, 10, 5, 0 };
    int start = 100;
    ReservationDefinition rDef =
        ReservationSystemTestUtil.createSimpleReservationDefinition(
            start, start + alloc.length + 1,
            alloc.length);
    Map<ReservationInterval, Resource> allocations =
        generateAllocation(start, alloc, true, false);
    int[] alloc = {};
    long start = 0;
    ReservationDefinition rDef =
        ReservationSystemTestUtil.createSimpleReservationDefinition(
            start, start + alloc.length + 1,
            alloc.length);
    Map<ReservationInterval, Resource> allocations =
        new HashMap<ReservationInterval, Resource>();
    int[] alloc = { 10, 10, 10, 10, 10, 10 };
    int start = 100;
    ReservationDefinition rDef =
        ReservationSystemTestUtil.createSimpleReservationDefinition(
            start, start + alloc.length + 1,
            alloc.length);
    boolean isGang = true;
    Map<ReservationInterval, Resource> allocations =
    Assert.assertEquals(start + alloc.length + 1, rAllocation.getEndTime());
  }

  private Map<ReservationInterval, Resource> generateAllocation(
      int startTime, int[] alloc, boolean isStep, boolean isGang) {
    Map<ReservationInterval, Resource> req =

