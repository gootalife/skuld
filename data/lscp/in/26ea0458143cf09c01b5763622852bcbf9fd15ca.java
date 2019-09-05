hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/reservation/AbstractReservationSystem.java
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.planning.Planner;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.planning.ReservationAgent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/reservation/InMemoryPlan.java
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions.PlanningException;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.planning.Planner;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.planning.ReservationAgent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.UTCClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InMemoryPlan implements Plan {

  private static final Logger LOG = LoggerFactory.getLogger(InMemoryPlan.class);


  private Resource totalCapacity;

  public InMemoryPlan(QueueMetrics queueMetrics, SharingPolicy policy,
      ReservationAgent agent, Resource totalCapacity, long step,
      ResourceCalculator resCalc, Resource minAlloc, Resource maxAlloc,
      String queueName, Planner replanner, boolean getMoveOnExpiry) {
        maxAlloc, queueName, replanner, getMoveOnExpiry, new UTCClock());
  }

  public InMemoryPlan(QueueMetrics queueMetrics, SharingPolicy policy,
      ReservationAgent agent, Resource totalCapacity, long step,
      ResourceCalculator resCalc, Resource minAlloc, Resource maxAlloc,
      String queueName, Planner replanner, boolean getMoveOnExpiry, Clock clock) {

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/reservation/InMemoryReservationAllocation.java
public class InMemoryReservationAllocation implements ReservationAllocation {

  private final String planName;
  private final ReservationId reservationID;

  private RLESparseResourceAllocation resourcesOverTime;

  public InMemoryReservationAllocation(ReservationId reservationID,
      ReservationDefinition contract, String user, String planName,
      long startTime, long endTime,
      Map<ReservationInterval, Resource> allocations,
        allocations, calculator, minAlloc, false);
  }

  public InMemoryReservationAllocation(ReservationId reservationID,
      ReservationDefinition contract, String user, String planName,
      long startTime, long endTime,
      Map<ReservationInterval, Resource> allocations,

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/reservation/Plan.java
package org.apache.hadoop.yarn.server.resourcemanager.reservation;

import org.apache.hadoop.yarn.api.records.ReservationDefinition;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.planning.ReservationAgent;


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/reservation/PlanContext.java
package org.apache.hadoop.yarn.server.resourcemanager.reservation;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.planning.Planner;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.planning.ReservationAgent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/reservation/PlanView.java
package org.apache.hadoop.yarn.server.resourcemanager.reservation;

import java.util.Set;

import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.planning.ReservationAgent;


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/reservation/RLESparseResourceAllocation.java

public class RLESparseResourceAllocation {

    }
  }

  public Map<ReservationInterval, Resource> toIntervalMap() {

    readLock.lock();
    try {
      Map<ReservationInterval, Resource> allocations =
          new TreeMap<ReservationInterval, Resource>();

      if (isEmpty()) {
        return allocations;
      }

      Map.Entry<Long, Resource> lastEntry = null;
      for (Map.Entry<Long, Resource> entry : cumulativeCapacity.entrySet()) {

        if (lastEntry != null) {
          ReservationInterval interval =
              new ReservationInterval(lastEntry.getKey(), entry.getKey());
          Resource resource = lastEntry.getValue();

          allocations.put(interval, resource);
        }

        lastEntry = entry;
      }
      return allocations;
    } finally {
      readLock.unlock();
    }

  }

}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/reservation/ReservationSchedulerConfiguration.java
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ReservationDefinition;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.planning.ReservationAgent;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.planning.Planner;

public abstract class ReservationSchedulerConfiguration extends Configuration {


  @InterfaceAudience.Private
  public static final String DEFAULT_RESERVATION_AGENT_NAME =
      "org.apache.hadoop.yarn.server.resourcemanager.reservation.planning.AlignedPlannerWithGreedy";

  @InterfaceAudience.Private
  public static final String DEFAULT_RESERVATION_PLANNER_NAME =
      "org.apache.hadoop.yarn.server.resourcemanager.reservation.planning.SimpleCapacityReplanner";

  @InterfaceAudience.Private
  public static final boolean DEFAULT_RESERVATION_MOVE_ON_EXPIRY = true;

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/reservation/ReservationSystem.java
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Queue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.planning.ReservationAgent;


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/reservation/ReservationSystemUtil.java
import java.util.HashMap;
import java.util.Map;

public final class ReservationSystemUtil {

  private ReservationSystemUtil() {

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/reservation/planning/AlignedPlannerWithGreedy.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/reservation/planning/AlignedPlannerWithGreedy.java

package org.apache.hadoop.yarn.server.resourcemanager.reservation.planning;

import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.yarn.api.records.ReservationDefinition;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.Plan;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions.PlanningException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AlignedPlannerWithGreedy implements ReservationAgent {

  private static final int DEFAULT_SMOOTHNESS_FACTOR = 10;

  private static final Logger LOG = LoggerFactory
      .getLogger(AlignedPlannerWithGreedy.class);

  private final ReservationAgent planner;

  public AlignedPlannerWithGreedy() {
    this(DEFAULT_SMOOTHNESS_FACTOR);
  }

  public AlignedPlannerWithGreedy(int smoothnessFactor) {

    List<ReservationAgent> listAlg = new LinkedList<ReservationAgent>();

    ReservationAgent algAligned =
        new IterativePlanner(new StageEarliestStartByDemand(),
            new StageAllocatorLowCostAligned(smoothnessFactor));
    listAlg.add(algAligned);

    ReservationAgent algGreedy =
        new IterativePlanner(new StageEarliestStartByJobArrival(),
            new StageAllocatorGreedy());
    listAlg.add(algGreedy);

    planner = new TryManyReservationAgents(listAlg);

  }

  @Override
  public boolean createReservation(ReservationId reservationId, String user,
      Plan plan, ReservationDefinition contract) throws PlanningException {

    LOG.info("placing the following ReservationRequest: " + contract);

    try {
      boolean res =
          planner.createReservation(reservationId, user, plan, contract);

      if (res) {
        LOG.info("OUTCOME: SUCCESS, Reservation ID: "
            + reservationId.toString() + ", Contract: " + contract.toString());
      } else {
        LOG.info("OUTCOME: FAILURE, Reservation ID: "
            + reservationId.toString() + ", Contract: " + contract.toString());
      }
      return res;
    } catch (PlanningException e) {
      LOG.info("OUTCOME: FAILURE, Reservation ID: " + reservationId.toString()
          + ", Contract: " + contract.toString());
      throw e;
    }

  }

  @Override
  public boolean updateReservation(ReservationId reservationId, String user,
      Plan plan, ReservationDefinition contract) throws PlanningException {

    LOG.info("updating the following ReservationRequest: " + contract);

    return planner.updateReservation(reservationId, user, plan, contract);

  }

  @Override
  public boolean deleteReservation(ReservationId reservationId, String user,
      Plan plan) throws PlanningException {

    LOG.info("removing the following ReservationId: " + reservationId);

    return planner.deleteReservation(reservationId, user, plan);

  }

}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/reservation/planning/GreedyReservationAgent.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/reservation/planning/GreedyReservationAgent.java

package org.apache.hadoop.yarn.server.resourcemanager.reservation.planning;

import org.apache.hadoop.yarn.api.records.ReservationDefinition;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.Plan;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions.PlanningException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class GreedyReservationAgent implements ReservationAgent {

  private static final Logger LOG = LoggerFactory
      .getLogger(GreedyReservationAgent.class);

  private final ReservationAgent planner = new IterativePlanner(
      new StageEarliestStartByJobArrival(), new StageAllocatorGreedy());

  @Override
  public boolean createReservation(ReservationId reservationId, String user,
      Plan plan, ReservationDefinition contract) throws PlanningException {

    LOG.info("placing the following ReservationRequest: " + contract);

    try {
      boolean res =
          planner.createReservation(reservationId, user, plan, contract);

      if (res) {
        LOG.info("OUTCOME: SUCCESS, Reservation ID: "
            + reservationId.toString() + ", Contract: " + contract.toString());
      } else {
        LOG.info("OUTCOME: FAILURE, Reservation ID: "
            + reservationId.toString() + ", Contract: " + contract.toString());
      }
      return res;
    } catch (PlanningException e) {
      LOG.info("OUTCOME: FAILURE, Reservation ID: " + reservationId.toString()
          + ", Contract: " + contract.toString());
      throw e;
    }

  }

  @Override
  public boolean updateReservation(ReservationId reservationId, String user,
      Plan plan, ReservationDefinition contract) throws PlanningException {

    LOG.info("updating the following ReservationRequest: " + contract);

    return planner.updateReservation(reservationId, user, plan, contract);

  }

  @Override
  public boolean deleteReservation(ReservationId reservationId, String user,
      Plan plan) throws PlanningException {

    LOG.info("removing the following ReservationId: " + reservationId);

    return planner.deleteReservation(reservationId, user, plan);

  }

}
\No newline at end of file

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/reservation/planning/IterativePlanner.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/reservation/planning/IterativePlanner.java

package org.apache.hadoop.yarn.server.resourcemanager.reservation.planning;

import java.util.HashMap;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.yarn.api.records.ReservationDefinition;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.ReservationRequest;
import org.apache.hadoop.yarn.api.records.ReservationRequestInterpreter;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.Plan;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.RLESparseResourceAllocation;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationAllocation;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationInterval;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions.ContractValidationException;
import org.apache.hadoop.yarn.util.resource.Resources;

public class IterativePlanner extends PlanningAlgorithm {

  private RLESparseResourceAllocation planModifications;

  private Map<Long, Resource> planLoads;
  private Resource capacity;
  private long step;

  private ReservationRequestInterpreter jobType;
  private long jobArrival;
  private long jobDeadline;

  private StageEarliestStart algStageEarliestStart = null;
  private StageAllocator algStageAllocator = null;

  public IterativePlanner(StageEarliestStart algEarliestStartTime,
      StageAllocator algStageAllocator) {

    setAlgStageEarliestStart(algEarliestStartTime);
    setAlgStageAllocator(algStageAllocator);

  }

  @Override
  public RLESparseResourceAllocation computeJobAllocation(Plan plan,
      ReservationId reservationId, ReservationDefinition reservation)
      throws ContractValidationException {

    initialize(plan, reservation);

    ReservationAllocation oldReservation =
        plan.getReservationById(reservationId);
    if (oldReservation != null) {
      ignoreOldAllocation(oldReservation);
    }

    RLESparseResourceAllocation allocations =
        new RLESparseResourceAllocation(plan.getResourceCalculator(),
            plan.getMinimumAllocation());

    ListIterator<ReservationRequest> li =
        reservation
            .getReservationRequests()
            .getReservationResources()
            .listIterator(
                reservation.getReservationRequests().getReservationResources()
                    .size());

    ReservationRequest currentReservationStage;

    int index =
        reservation.getReservationRequests().getReservationResources().size();

    long stageDeadline = stepRoundDown(reservation.getDeadline(), step);
    long successorStartingTime = -1;

    while (li.hasPrevious()) {

      currentReservationStage = li.previous();
      index -= 1;

      validateInputStage(plan, currentReservationStage);

      long stageArrivalTime = reservation.getArrival();
      if (jobType == ReservationRequestInterpreter.R_ORDER
          || jobType == ReservationRequestInterpreter.R_ORDER_NO_GAP) {
        stageArrivalTime =
            computeEarliestStartingTime(plan, reservation, index,
                currentReservationStage, stageDeadline);
      }
      stageArrivalTime = stepRoundUp(stageArrivalTime, step);
      stageArrivalTime = Math.max(stageArrivalTime, reservation.getArrival());

      Map<ReservationInterval, Resource> curAlloc =
          computeStageAllocation(plan, currentReservationStage,
              stageArrivalTime, stageDeadline);

      if (curAlloc == null) {

        if (jobType == ReservationRequestInterpreter.R_ANY) {
          continue;
        }

        return null;

      }

      Long stageStartTime = findEarliestTime(curAlloc.keySet());
      Long stageEndTime = findLatestTime(curAlloc.keySet());

      for (Entry<ReservationInterval, Resource> entry : curAlloc.entrySet()) {
        allocations.addInterval(entry.getKey(), entry.getValue());
      }

      if (jobType == ReservationRequestInterpreter.R_ANY) {
        break;
      }

      if (jobType == ReservationRequestInterpreter.R_ORDER
          || jobType == ReservationRequestInterpreter.R_ORDER_NO_GAP) {

        if (jobType == ReservationRequestInterpreter.R_ORDER_NO_GAP
            && successorStartingTime != -1
            && successorStartingTime > stageEndTime) {

          return null;

        }

        successorStartingTime = stageStartTime;
        stageDeadline = stageStartTime;

      }

    }

    if (allocations.isEmpty()) {
      return null;
    }

    return allocations;

  }

  protected void initialize(Plan plan, ReservationDefinition reservation) {

    capacity = plan.getTotalCapacity();
    step = plan.getStep();

    jobType = reservation.getReservationRequests().getInterpreter();
    jobArrival = stepRoundUp(reservation.getArrival(), step);
    jobDeadline = stepRoundDown(reservation.getDeadline(), step);

    planLoads = getAllLoadsInInterval(plan, jobArrival, jobDeadline);

    planModifications =
        new RLESparseResourceAllocation(plan.getResourceCalculator(),
            plan.getMinimumAllocation());

  }

  private Map<Long, Resource> getAllLoadsInInterval(Plan plan, long startTime,
      long endTime) {

    Map<Long, Resource> loads = new HashMap<Long, Resource>();

    for (long t = startTime; t < endTime; t += step) {
      Resource load = plan.getTotalCommittedResources(t);
      loads.put(t, load);
    }

    return loads;

  }

  private void ignoreOldAllocation(ReservationAllocation oldReservation) {

    if (oldReservation == null) {
      return;
    }

    for (Entry<ReservationInterval, Resource> entry : oldReservation
        .getAllocationRequests().entrySet()) {

      ReservationInterval interval = entry.getKey();
      Resource resource = entry.getValue();

      Resource negativeResource = Resources.multiply(resource, -1);

      planModifications.addInterval(interval, negativeResource);

    }

  }

  private void validateInputStage(Plan plan, ReservationRequest rr)
      throws ContractValidationException {

    if (rr.getConcurrency() < 1) {
      throw new ContractValidationException("Gang Size should be >= 1");
    }

    if (rr.getNumContainers() <= 0) {
      throw new ContractValidationException("Num containers should be > 0");
    }

    if (rr.getNumContainers() % rr.getConcurrency() != 0) {
      throw new ContractValidationException(
          "Parallelism must be an exact multiple of gang size");
    }

    if (Resources.greaterThan(plan.getResourceCalculator(), capacity,
        rr.getCapability(), plan.getMaximumAllocation())) {

      throw new ContractValidationException(
          "Individual capability requests should not exceed cluster's " +
          "maxAlloc");

    }

  }

  protected long computeEarliestStartingTime(Plan plan,
      ReservationDefinition reservation, int index,
      ReservationRequest currentReservationStage, long stageDeadline) {

    return algStageEarliestStart.setEarliestStartTime(plan, reservation, index,
        currentReservationStage, stageDeadline);

  }

  protected Map<ReservationInterval, Resource> computeStageAllocation(
      Plan plan, ReservationRequest rr, long stageArrivalTime,
      long stageDeadline) {

    return algStageAllocator.computeStageAllocation(plan, planLoads,
        planModifications, rr, stageArrivalTime, stageDeadline);

  }

  public IterativePlanner setAlgStageEarliestStart(StageEarliestStart alg) {

    this.algStageEarliestStart = alg;
    return this; // To allow concatenation of setAlg() functions

  }

  public IterativePlanner setAlgStageAllocator(StageAllocator alg) {

    this.algStageAllocator = alg;
    return this; // To allow concatenation of setAlg() functions

  }

}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/reservation/planning/Planner.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/reservation/planning/Planner.java

package org.apache.hadoop.yarn.server.resourcemanager.reservation.planning;

import java.util.List;

import org.apache.hadoop.yarn.api.records.ReservationDefinition;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.Plan;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationSchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions.PlanningException;

public interface Planner {

  public void plan(Plan plan, List<ReservationDefinition> contracts)
      throws PlanningException;

  void init(String planQueueName, ReservationSchedulerConfiguration conf);
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/reservation/planning/PlanningAlgorithm.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/reservation/planning/PlanningAlgorithm.java

package org.apache.hadoop.yarn.server.resourcemanager.reservation.planning;

import java.util.Map;
import java.util.Set;

import org.apache.hadoop.yarn.api.records.ReservationDefinition;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.InMemoryReservationAllocation;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.Plan;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.RLESparseResourceAllocation;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationAllocation;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationInterval;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions.ContractValidationException;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions.PlanningException;

public abstract class PlanningAlgorithm implements ReservationAgent {

  protected boolean allocateUser(ReservationId reservationId, String user,
      Plan plan, ReservationDefinition contract,
      ReservationAllocation oldReservation) throws PlanningException,
      ContractValidationException {

    ReservationDefinition adjustedContract = adjustContract(plan, contract);

    RLESparseResourceAllocation allocation =
        computeJobAllocation(plan, reservationId, adjustedContract);

    if (allocation == null) {
      throw new PlanningException(
          "The planning algorithm could not find a valid allocation"
              + " for your request");
    }

    long step = plan.getStep();
    long jobArrival = stepRoundUp(adjustedContract.getArrival(), step);
    long jobDeadline = stepRoundUp(adjustedContract.getDeadline(), step);
    Map<ReservationInterval, Resource> mapAllocations =
        allocationsToPaddedMap(allocation, jobArrival, jobDeadline);

    ReservationAllocation capReservation =
        new InMemoryReservationAllocation(reservationId, // ID
            adjustedContract, // Contract
            user, // User name
            plan.getQueueName(), // Queue name
            findEarliestTime(mapAllocations.keySet()), // Earliest start time
            findLatestTime(mapAllocations.keySet()), // Latest end time
            mapAllocations, // Allocations
            plan.getResourceCalculator(), // Resource calculator
            plan.getMinimumAllocation()); // Minimum allocation

    if (oldReservation != null) {
      return plan.updateReservation(capReservation);
    } else {
      return plan.addReservation(capReservation);
    }

  }

  private Map<ReservationInterval, Resource>
      allocationsToPaddedMap(RLESparseResourceAllocation allocation,
          long jobArrival, long jobDeadline) {

    Map<ReservationInterval, Resource> mapAllocations =
        allocation.toIntervalMap();

    Resource zeroResource = Resource.newInstance(0, 0);

    long earliestStart = findEarliestTime(mapAllocations.keySet());
    if (jobArrival < earliestStart) {
      mapAllocations.put(new ReservationInterval(jobArrival, earliestStart),
          zeroResource);
    }

    long latestEnd = findLatestTime(mapAllocations.keySet());
    if (latestEnd < jobDeadline) {
      mapAllocations.put(new ReservationInterval(latestEnd, jobDeadline),
          zeroResource);
    }

    return mapAllocations;

  }

  public abstract RLESparseResourceAllocation computeJobAllocation(Plan plan,
      ReservationId reservationId, ReservationDefinition reservation)
      throws PlanningException, ContractValidationException;

  @Override
  public boolean createReservation(ReservationId reservationId, String user,
      Plan plan, ReservationDefinition contract) throws PlanningException {

    return allocateUser(reservationId, user, plan, contract, null);

  }

  @Override
  public boolean updateReservation(ReservationId reservationId, String user,
      Plan plan, ReservationDefinition contract) throws PlanningException {

    ReservationAllocation oldAlloc = plan.getReservationById(reservationId);

    return allocateUser(reservationId, user, plan, contract, oldAlloc);

  }

  @Override
  public boolean deleteReservation(ReservationId reservationId, String user,
      Plan plan) throws PlanningException {

    return plan.deleteReservation(reservationId);

  }

  protected static long findEarliestTime(Set<ReservationInterval> sesInt) {

    long ret = Long.MAX_VALUE;
    for (ReservationInterval s : sesInt) {
      if (s.getStartTime() < ret) {
        ret = s.getStartTime();
      }
    }
    return ret;

  }

  protected static long findLatestTime(Set<ReservationInterval> sesInt) {

    long ret = Long.MIN_VALUE;
    for (ReservationInterval s : sesInt) {
      if (s.getEndTime() > ret) {
        ret = s.getEndTime();
      }
    }
    return ret;

  }

  protected static long stepRoundDown(long t, long step) {
    return (t / step) * step;
  }

  protected static long stepRoundUp(long t, long step) {
    return ((t + step - 1) / step) * step;
  }

  private ReservationDefinition adjustContract(Plan plan,
      ReservationDefinition originalContract) {


    return originalContract;

  }

}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/reservation/planning/ReservationAgent.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/reservation/planning/ReservationAgent.java
package org.apache.hadoop.yarn.server.resourcemanager.reservation.planning;

import org.apache.hadoop.yarn.api.records.ReservationDefinition;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.Plan;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions.PlanningException;

public interface ReservationAgent {

  public boolean createReservation(ReservationId reservationId, String user,
      Plan plan, ReservationDefinition contract) throws PlanningException;

  public boolean updateReservation(ReservationId reservationId, String user,
      Plan plan, ReservationDefinition contract) throws PlanningException;

  public boolean deleteReservation(ReservationId reservationId, String user,
      Plan plan) throws PlanningException;

}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/reservation/planning/SimpleCapacityReplanner.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/reservation/planning/SimpleCapacityReplanner.java

package org.apache.hadoop.yarn.server.resourcemanager.reservation.planning;

import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ReservationDefinition;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.Plan;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationAllocation;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationSchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions.PlanningException;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.UTCClock;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

import com.google.common.annotations.VisibleForTesting;

public class SimpleCapacityReplanner implements Planner {

  private static final Log LOG = LogFactory
      .getLog(SimpleCapacityReplanner.class);

  private static final Resource ZERO_RESOURCE = Resource.newInstance(0, 0);

  private final Clock clock;

  private long lengthOfCheckZone;

  public SimpleCapacityReplanner() {
    this(new UTCClock());
  }

  @VisibleForTesting
  SimpleCapacityReplanner(Clock clock) {
    this.clock = clock;
  }

  @Override
  public void init(String planQueueName,
      ReservationSchedulerConfiguration conf) {
    this.lengthOfCheckZone = conf.getEnforcementWindow(planQueueName);
  }

  @Override
  public void plan(Plan plan, List<ReservationDefinition> contracts)
      throws PlanningException {

    if (contracts != null) {
      throw new RuntimeException(
          "SimpleCapacityReplanner cannot handle new reservation contracts");
    }

    ResourceCalculator resCalc = plan.getResourceCalculator();
    Resource totCap = plan.getTotalCapacity();
    long now = clock.getTime();

    for (long t = now; 
         (t < plan.getLastEndTime() && t < (now + lengthOfCheckZone)); 
         t += plan.getStep()) {
      Resource excessCap =
          Resources.subtract(plan.getTotalCommittedResources(t), totCap);
      if (Resources.greaterThan(resCalc, totCap, excessCap, ZERO_RESOURCE)) {
        Set<ReservationAllocation> curReservations =
            new TreeSet<ReservationAllocation>(plan.getReservationsAtTime(t));
        for (Iterator<ReservationAllocation> resIter =
            curReservations.iterator(); resIter.hasNext()
            && Resources.greaterThan(resCalc, totCap, excessCap, 
                ZERO_RESOURCE);) {
          ReservationAllocation reservation = resIter.next();
          plan.deleteReservation(reservation.getReservationId());
          excessCap =
              Resources.subtract(excessCap, reservation.getResourcesAtTime(t));
          LOG.info("Removing reservation " + reservation.getReservationId()
              + " to repair physical-resource constraints in the plan: "
              + plan.getQueueName());
        }
      }
    }
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/reservation/planning/StageAllocator.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/reservation/planning/StageAllocator.java

package org.apache.hadoop.yarn.server.resourcemanager.reservation.planning;

import java.util.Map;

import org.apache.hadoop.yarn.api.records.ReservationRequest;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.Plan;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.RLESparseResourceAllocation;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationInterval;

public interface StageAllocator {

  Map<ReservationInterval, Resource> computeStageAllocation(Plan plan,
      Map<Long, Resource> planLoads,
      RLESparseResourceAllocation planModifications, ReservationRequest rr,
      long stageEarliestStart, long stageDeadline);

}
\No newline at end of file

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/reservation/planning/StageAllocatorGreedy.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/reservation/planning/StageAllocatorGreedy.java

package org.apache.hadoop.yarn.server.resourcemanager.reservation.planning;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.yarn.api.records.ReservationRequest;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.Plan;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.RLESparseResourceAllocation;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationInterval;
import org.apache.hadoop.yarn.util.resource.Resources;


public class StageAllocatorGreedy implements StageAllocator {

  @Override
  public Map<ReservationInterval, Resource> computeStageAllocation(Plan plan,
      Map<Long, Resource> planLoads,
      RLESparseResourceAllocation planModifications, ReservationRequest rr,
      long stageEarliestStart, long stageDeadline) {

    Resource totalCapacity = plan.getTotalCapacity();

    Map<ReservationInterval, Resource> allocationRequests =
        new HashMap<ReservationInterval, Resource>();

    Resource gang = Resources.multiply(rr.getCapability(), rr.getConcurrency());
    long dur = rr.getDuration();
    long step = plan.getStep();

    if (dur % step != 0) {
      dur += (step - (dur % step));
    }

    int gangsToPlace = rr.getNumContainers() / rr.getConcurrency();

    int maxGang = 0;

    while (gangsToPlace > 0 && stageDeadline - dur >= stageEarliestStart) {

      maxGang = gangsToPlace;
      long minPoint = stageDeadline;
      int curMaxGang = maxGang;

      for (long t = stageDeadline - plan.getStep(); t >= stageDeadline - dur
          && maxGang > 0; t = t - plan.getStep()) {

        Resource netAvailableRes = Resources.clone(totalCapacity);
        Resources.subtractFrom(netAvailableRes,
            plan.getTotalCommittedResources(t));
        Resources.subtractFrom(netAvailableRes,
            planModifications.getCapacityAtTime(t));

        curMaxGang =
            (int) Math.floor(Resources.divide(plan.getResourceCalculator(),
                totalCapacity, netAvailableRes, gang));

        curMaxGang = Math.min(gangsToPlace, curMaxGang);

        if (curMaxGang <= maxGang) {
          maxGang = curMaxGang;
          minPoint = t;
        }
      }

      if (maxGang > 0) {
        gangsToPlace -= maxGang;

        ReservationInterval reservationInt =
            new ReservationInterval(stageDeadline - dur, stageDeadline);
        Resource reservationRes =
            Resources.multiply(rr.getCapability(), rr.getConcurrency()
        planModifications.addInterval(reservationInt, reservationRes);
        allocationRequests.put(reservationInt, reservationRes);

      }

      stageDeadline = minPoint;
    }

    if (gangsToPlace == 0) {
      return allocationRequests;
    } else {
      for (Map.Entry<ReservationInterval, Resource> tempAllocation
          : allocationRequests.entrySet()) {
        planModifications.removeInterval(tempAllocation.getKey(),
            tempAllocation.getValue());
      }
      return null;
    }

  }

}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/reservation/planning/StageAllocatorLowCostAligned.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/reservation/planning/StageAllocatorLowCostAligned.java

package org.apache.hadoop.yarn.server.resourcemanager.reservation.planning;

import java.util.Comparator;
import java.util.Map;
import java.util.TreeSet;

import org.apache.hadoop.yarn.api.records.ReservationRequest;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.Plan;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.RLESparseResourceAllocation;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationInterval;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;


public class StageAllocatorLowCostAligned implements StageAllocator {

  private int smoothnessFactor = 10;

  public StageAllocatorLowCostAligned() {
  }

  public StageAllocatorLowCostAligned(int smoothnessFactor) {
    this.smoothnessFactor = smoothnessFactor;
  }

  @Override
  public Map<ReservationInterval, Resource> computeStageAllocation(
      Plan plan, Map<Long, Resource> planLoads,
      RLESparseResourceAllocation planModifications, ReservationRequest rr,
      long stageEarliestStart, long stageDeadline) {

    ResourceCalculator resCalc = plan.getResourceCalculator();
    Resource capacity = plan.getTotalCapacity();
    long step = plan.getStep();

    RLESparseResourceAllocation allocationRequests =
        new RLESparseResourceAllocation(plan.getResourceCalculator(),
            plan.getMinimumAllocation());

    long duration = stepRoundUp(rr.getDuration(), step);
    int windowSizeInDurations =
        (int) ((stageDeadline - stageEarliestStart) / duration);
    int totalGangs = rr.getNumContainers() / rr.getConcurrency();
    int numContainersPerGang = rr.getConcurrency();
    Resource gang =
        Resources.multiply(rr.getCapability(), numContainersPerGang);

    int maxGangsPerUnit =
        (int) Math.max(
            Math.floor(((double) totalGangs) / windowSizeInDurations), 1);
    maxGangsPerUnit = Math.max(maxGangsPerUnit / smoothnessFactor, 1);

    if (windowSizeInDurations <= 0) {
      return null;
    }

    TreeSet<DurationInterval> durationIntervalsSortedByCost =
        new TreeSet<DurationInterval>(new Comparator<DurationInterval>() {
          @Override
          public int compare(DurationInterval val1, DurationInterval val2) {

            int cmp = Double.compare(val1.getTotalCost(), val2.getTotalCost());
            if (cmp != 0) {
              return cmp;
            }

            return (-1) * Long.compare(val1.getEndTime(), val2.getEndTime());
          }
        });

    for (long intervalEnd = stageDeadline; intervalEnd >= stageEarliestStart
        + duration; intervalEnd -= duration) {

      long intervalStart = intervalEnd - duration;

      DurationInterval durationInterval =
          getDurationInterval(intervalStart, intervalEnd, planLoads,
              planModifications, capacity, resCalc, step);

      if (durationInterval.canAllocate(gang, capacity, resCalc)) {
        durationIntervalsSortedByCost.add(durationInterval);
      }
    }

    int remainingGangs = totalGangs;
    while (remainingGangs > 0) {

      if (durationIntervalsSortedByCost.isEmpty()) {
        break;
      }

      DurationInterval bestDurationInterval =
          durationIntervalsSortedByCost.first();
      int numGangsToAllocate = Math.min(maxGangsPerUnit, remainingGangs);

      remainingGangs -= numGangsToAllocate;

      ReservationInterval reservationInt =
          new ReservationInterval(bestDurationInterval.getStartTime(),
              bestDurationInterval.getEndTime());

      Resource reservationRes =
          Resources.multiply(rr.getCapability(), rr.getConcurrency()

      planModifications.addInterval(reservationInt, reservationRes);
      allocationRequests.addInterval(reservationInt, reservationRes);

      durationIntervalsSortedByCost.remove(bestDurationInterval);

      DurationInterval updatedDurationInterval =
          getDurationInterval(bestDurationInterval.getStartTime(),
              bestDurationInterval.getStartTime() + duration, planLoads,
              planModifications, capacity, resCalc, step);

      if (updatedDurationInterval.canAllocate(gang, capacity, resCalc)) {
        durationIntervalsSortedByCost.add(updatedDurationInterval);
      }

    }

    Map<ReservationInterval, Resource> allocations =
        allocationRequests.toIntervalMap();

    if (remainingGangs <= 0) {
      return allocations;
    } else {

      for (Map.Entry<ReservationInterval, Resource> tempAllocation
          : allocations.entrySet()) {

        planModifications.removeInterval(tempAllocation.getKey(),
            tempAllocation.getValue());

      }
      return null;

    }

  }

  protected DurationInterval getDurationInterval(long startTime, long endTime,
      Map<Long, Resource> planLoads,
      RLESparseResourceAllocation planModifications, Resource capacity,
      ResourceCalculator resCalc, long step) {

    Resource dominantResources = Resource.newInstance(0, 0);

    double totalCost = 0.0;
    for (long t = startTime; t < endTime; t += step) {

      Resource load = getLoadAtTime(t, planLoads, planModifications);

      totalCost += calcCostOfLoad(load, capacity, resCalc);

      dominantResources = Resources.componentwiseMax(dominantResources, load);

    }

    return new DurationInterval(startTime, endTime, totalCost,
        dominantResources);

  }

  protected double calcCostOfInterval(long startTime, long endTime,
      Map<Long, Resource> planLoads,
      RLESparseResourceAllocation planModifications, Resource capacity,
      ResourceCalculator resCalc, long step) {

    double totalCost = 0.0;
    for (long t = startTime; t < endTime; t += step) {
      totalCost += calcCostOfTimeSlot(t, planLoads, planModifications, capacity,
          resCalc);
    }

    return totalCost;

  }

  protected double calcCostOfTimeSlot(long t, Map<Long, Resource> planLoads,
      RLESparseResourceAllocation planModifications, Resource capacity,
      ResourceCalculator resCalc) {

    Resource load = getLoadAtTime(t, planLoads, planModifications);

    return calcCostOfLoad(load, capacity, resCalc);

  }

  protected Resource getLoadAtTime(long t, Map<Long, Resource> planLoads,
      RLESparseResourceAllocation planModifications) {

    Resource planLoad = planLoads.get(t);
    planLoad = (planLoad == null) ? Resource.newInstance(0, 0) : planLoad;

    return Resources.add(planLoad, planModifications.getCapacityAtTime(t));

  }

  protected double calcCostOfLoad(Resource load, Resource capacity,
      ResourceCalculator resCalc) {

    return resCalc.ratio(load, capacity);

  }

  protected static long stepRoundDown(long t, long step) {
    return (t / step) * step;
  }

  protected static long stepRoundUp(long t, long step) {
    return ((t + step - 1) / step) * step;
  }

  protected static class DurationInterval {

    private long startTime;
    private long endTime;
    private double cost;
    private Resource maxLoad;

    public DurationInterval(long startTime, long endTime, double cost,
        Resource maxLoad) {
      this.startTime = startTime;
      this.endTime = endTime;
      this.cost = cost;
      this.maxLoad = maxLoad;
    }

    public boolean canAllocate(Resource requestedResources, Resource capacity,
        ResourceCalculator resCalc) {

      Resource updatedMaxLoad = Resources.add(maxLoad, requestedResources);
      return (resCalc.compare(capacity, updatedMaxLoad, capacity) <= 0);

    }

    public int numCanFit(Resource requestedResources, Resource capacity,
        ResourceCalculator resCalc) {

      Resource availableResources = Resources.subtract(capacity, maxLoad);

      return (int) Math.floor(Resources.divide(resCalc, capacity,
          availableResources, requestedResources));

    }

    public long getStartTime() {
      return this.startTime;
    }

    public void setStartTime(long value) {
      this.startTime = value;
    }

    public long getEndTime() {
      return this.endTime;
    }

    public void setEndTime(long value) {
      this.endTime = value;
    }

    public Resource getMaxLoad() {
      return this.maxLoad;
    }

    public void setMaxLoad(Resource value) {
      this.maxLoad = value;
    }

    public double getTotalCost() {
      return this.cost;
    }

    public void setTotalCost(double value) {
      this.cost = value;
    }

  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/reservation/planning/StageEarliestStart.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/reservation/planning/StageEarliestStart.java

package org.apache.hadoop.yarn.server.resourcemanager.reservation.planning;

import org.apache.hadoop.yarn.api.records.ReservationDefinition;
import org.apache.hadoop.yarn.api.records.ReservationRequest;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.Plan;

public interface StageEarliestStart {

  long setEarliestStartTime(Plan plan, ReservationDefinition reservation,
          int index, ReservationRequest currentReservationStage,
          long stageDeadline);

}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/reservation/planning/StageEarliestStartByDemand.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/reservation/planning/StageEarliestStartByDemand.java

package org.apache.hadoop.yarn.server.resourcemanager.reservation.planning;

import java.util.ListIterator;

import org.apache.hadoop.yarn.api.records.ReservationDefinition;
import org.apache.hadoop.yarn.api.records.ReservationRequest;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.Plan;


public class StageEarliestStartByDemand implements StageEarliestStart {

  private long step;

  @Override
  public long setEarliestStartTime(Plan plan,
      ReservationDefinition reservation, int index, ReservationRequest current,
      long stageDeadline) {

    step = plan.getStep();

    if (index < 1) {
      return reservation.getArrival();
    }

    ListIterator<ReservationRequest> li =
        reservation.getReservationRequests().getReservationResources()
            .listIterator(index);
    ReservationRequest rr;

    double totalWeight = calcWeight(current);
    long totalDuration = getRoundedDuration(current, plan);

    while (li.hasPrevious()) {
      rr = li.previous();
      totalWeight += calcWeight(rr);
      totalDuration += getRoundedDuration(rr, plan);
    }

    double ratio = calcWeight(current) / totalWeight;

    long window = stageDeadline - reservation.getArrival();
    long windowRemainder = window - totalDuration;
    long earlyStart =
        (long) (stageDeadline - getRoundedDuration(current, plan)
            - (windowRemainder * ratio));

    earlyStart = stepRoundUp(earlyStart, step);

    return earlyStart;

  }

  protected double calcWeight(ReservationRequest stage) {
    return (stage.getDuration() * stage.getCapability().getMemory())
  }

  protected long getRoundedDuration(ReservationRequest stage, Plan plan) {
    return stepRoundUp(stage.getDuration(), step);
  }

  protected static long stepRoundDown(long t, long step) {
    return (t / step) * step;
  }

  protected static long stepRoundUp(long t, long step) {
    return ((t + step - 1) / step) * step;
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/reservation/planning/StageEarliestStartByJobArrival.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/reservation/planning/StageEarliestStartByJobArrival.java

package org.apache.hadoop.yarn.server.resourcemanager.reservation.planning;

import org.apache.hadoop.yarn.api.records.ReservationDefinition;
import org.apache.hadoop.yarn.api.records.ReservationRequest;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.Plan;

public class StageEarliestStartByJobArrival implements StageEarliestStart {

  @Override
  public long setEarliestStartTime(Plan plan,
      ReservationDefinition reservation, int index, ReservationRequest current,
      long stageDeadline) {

    return reservation.getArrival();

  }

}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/reservation/planning/TryManyReservationAgents.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/reservation/planning/TryManyReservationAgents.java

package org.apache.hadoop.yarn.server.resourcemanager.reservation.planning;

import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.yarn.api.records.ReservationDefinition;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.Plan;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions.PlanningException;

public class TryManyReservationAgents implements ReservationAgent {

  private final List<ReservationAgent> algs;

  public TryManyReservationAgents(List<ReservationAgent> algs) {
    this.algs = new LinkedList<ReservationAgent>(algs);
  }

  @Override
  public boolean createReservation(ReservationId reservationId, String user,
      Plan plan, ReservationDefinition contract) throws PlanningException {

    PlanningException planningException = null;

    for (ReservationAgent alg : algs) {

      try {
        if (alg.createReservation(reservationId, user, plan, contract)) {
          return true;
        }
      } catch (PlanningException e) {
        planningException = e;
      }

    }

    if (planningException != null) {
      throw planningException;
    }

    return false;

  }

  @Override
  public boolean updateReservation(ReservationId reservationId, String user,
      Plan plan, ReservationDefinition contract) throws PlanningException {

    PlanningException planningException = null;

    for (ReservationAgent alg : algs) {

      try {
        if (alg.updateReservation(reservationId, user, plan, contract)) {
          return true;
        }
      } catch (PlanningException e) {
        planningException = e;
      }

    }

    if (planningException != null) {
      throw planningException;
    }

    return false;

  }

  @Override
  public boolean deleteReservation(ReservationId reservationId, String user,
      Plan plan) throws PlanningException {

    return plan.deleteReservation(reservationId);

  }

}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/reservation/ReservationSystemTestUtil.java
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.RMContextImpl;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.planning.AlignedPlannerWithGreedy;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AbstractYarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
    Assert.assertEquals(planQName, plan.getQueueName());
    Assert.assertEquals(8192, plan.getTotalCapacity().getMemory());
    Assert.assertTrue(
        plan.getReservationAgent() instanceof AlignedPlannerWithGreedy);
    Assert.assertTrue(
        plan.getSharingPolicy() instanceof CapacityOverTimePolicy);
  }
    Assert.assertEquals(newQ, newPlan.getQueueName());
    Assert.assertEquals(1024, newPlan.getTotalCapacity().getMemory());
    Assert
        .assertTrue(newPlan.getReservationAgent() instanceof AlignedPlannerWithGreedy);
    Assert
        .assertTrue(newPlan.getSharingPolicy() instanceof CapacityOverTimePolicy);
  }

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/reservation/TestCapacityOverTimePolicy.java
import org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions.PlanningException;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions.PlanningQuotaException;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions.ResourceOverCommitException;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.planning.ReservationAgent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/reservation/TestCapacitySchedulerPlanFollower.java
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions.PlanningException;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.planning.ReservationAgent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Queue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/reservation/TestFairReservationSystem.java
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/reservation/TestFairSchedulerPlanFollower.java
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions.PlanningException;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.planning.ReservationAgent;
import org.apache.hadoop.yarn.server.resourcemanager.resource.ResourceType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Queue;

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/reservation/TestInMemoryPlan.java
import org.apache.hadoop.yarn.api.records.impl.pb.ReservationDefinitionPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ReservationRequestsPBImpl;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions.PlanningException;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.planning.Planner;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.planning.ReservationAgent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/reservation/TestNoOverCommitPolicy.java
import org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions.MismatchedUserException;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions.PlanningException;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions.ResourceOverCommitException;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.planning.ReservationAgent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/reservation/TestRLESparseResourceAllocation.java
    Assert.assertTrue(rleSparseVector.isEmpty());
  }

  @Test
  public void testToIntervalMap() {
    ResourceCalculator resCalc = new DefaultResourceCalculator();
    Resource minAlloc = Resource.newInstance(1, 1);
    RLESparseResourceAllocation rleSparseVector =
        new RLESparseResourceAllocation(resCalc, minAlloc);
    Map<ReservationInterval, Resource> mapAllocations;

    mapAllocations = rleSparseVector.toIntervalMap();
    Assert.assertTrue(mapAllocations.isEmpty());

    int[] alloc = { 0, 5, 10, 10, 5, 0, 5, 0 };
    int start = 100;
    Set<Entry<ReservationInterval, Resource>> inputs =
        generateAllocation(start, alloc, false).entrySet();
    for (Entry<ReservationInterval, Resource> ip : inputs) {
      rleSparseVector.addInterval(ip.getKey(), ip.getValue());
    }
    mapAllocations = rleSparseVector.toIntervalMap();
    Assert.assertTrue(mapAllocations.size() == 5);
    for (Entry<ReservationInterval, Resource> entry : mapAllocations
        .entrySet()) {
      ReservationInterval interval = entry.getKey();
      Resource resource = entry.getValue();
      if (interval.getStartTime() == 101L) {
        Assert.assertTrue(interval.getEndTime() == 102L);
        Assert.assertEquals(resource, Resource.newInstance(5 * 1024, 5));
      } else if (interval.getStartTime() == 102L) {
        Assert.assertTrue(interval.getEndTime() == 104L);
        Assert.assertEquals(resource, Resource.newInstance(10 * 1024, 10));
      } else if (interval.getStartTime() == 104L) {
        Assert.assertTrue(interval.getEndTime() == 105L);
        Assert.assertEquals(resource, Resource.newInstance(5 * 1024, 5));
      } else if (interval.getStartTime() == 105L) {
        Assert.assertTrue(interval.getEndTime() == 106L);
        Assert.assertEquals(resource, Resource.newInstance(0 * 1024, 0));
      } else if (interval.getStartTime() == 106L) {
        Assert.assertTrue(interval.getEndTime() == 107L);
        Assert.assertEquals(resource, Resource.newInstance(5 * 1024, 5));
      } else {
        Assert.fail();
      }
    }
  }

  private Map<ReservationInterval, Resource> generateAllocation(
      int startTime, int[] alloc, boolean isStep) {
    Map<ReservationInterval, Resource> req =

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/reservation/TestSchedulerPlanFollowerBase.java
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions.PlanningException;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.planning.ReservationAgent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Queue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/reservation/planning/TestAlignedPlanner.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/reservation/planning/TestAlignedPlanner.java

package org.apache.hadoop.yarn.server.resourcemanager.reservation.planning;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.yarn.api.records.ReservationDefinition;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.ReservationRequest;
import org.apache.hadoop.yarn.api.records.ReservationRequestInterpreter;
import org.apache.hadoop.yarn.api.records.ReservationRequests;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.CapacityOverTimePolicy;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.InMemoryPlan;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.InMemoryReservationAllocation;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationAllocation;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationSchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationSystemTestUtil;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions.PlanningException;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.planning.AlignedPlannerWithGreedy;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.planning.ReservationAgent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.Before;
import org.junit.Test;
import org.mortbay.log.Log;

public class TestAlignedPlanner {

  ReservationAgent agent;
  InMemoryPlan plan;
  Resource minAlloc = Resource.newInstance(1024, 1);
  ResourceCalculator res = new DefaultResourceCalculator();
  Resource maxAlloc = Resource.newInstance(1024 * 8, 8);
  Random rand = new Random();
  long step;

  @Test
  public void testSingleReservationAccept() throws PlanningException {

    int numJobsInScenario = initializeScenario1();

    ReservationDefinition rr1 =
        createReservationDefinition(
            5 * step, // Job arrival time
            20 * step, // Job deadline
            new ReservationRequest[] { ReservationRequest.newInstance(
                Resource.newInstance(2048, 2), // Capability
                10, // Num containers
                5, // Concurrency
                10 * step) }, // Duration
            ReservationRequestInterpreter.R_ORDER, "u1");

    ReservationId reservationID =
        ReservationSystemTestUtil.getNewReservationId();
    agent.createReservation(reservationID, "u1", plan, rr1);

    assertTrue("Agent-based allocation failed", reservationID != null);
    assertTrue("Agent-based allocation failed", plan.getAllReservations()
        .size() == numJobsInScenario + 1);

    ReservationAllocation alloc1 = plan.getReservationById(reservationID);

    assertTrue(alloc1.toString(),
        check(alloc1, 10 * step, 20 * step, 10, 2048, 2));

  }

  @Test
  public void testOrderNoGapImpossible() throws PlanningException {

    int numJobsInScenario = initializeScenario2();

    ReservationDefinition rr1 =
        createReservationDefinition(
            10L, // Job arrival time
            15 * step, // Job deadline
            new ReservationRequest[] {
                ReservationRequest.newInstance(
                    Resource.newInstance(1024, 1), // Capability
                    20, // Num containers
                    20, // Concurrency
                    step), // Duration
                ReservationRequest.newInstance(
                    Resource.newInstance(1024, 1), // Capability
                    20, // Num containers
                    20, // Concurrency
                    step) }, // Duration
            ReservationRequestInterpreter.R_ORDER_NO_GAP, "u1");

    try {
      ReservationId reservationID =
          ReservationSystemTestUtil.getNewReservationId();
      agent.createReservation(reservationID, "u1", plan, rr1);
      fail();
    } catch (PlanningException e) {
    }

    assertTrue("Agent-based allocation should have failed", plan
        .getAllReservations().size() == numJobsInScenario);

  }

  @Test
  public void testOrderNoGapImpossible2() throws PlanningException {

    int numJobsInScenario = initializeScenario2();

    ReservationDefinition rr1 =
        createReservationDefinition(
            10 * step, // Job arrival time
            13 * step, // Job deadline
            new ReservationRequest[] {
                ReservationRequest.newInstance(
                    Resource.newInstance(1024, 1), // Capability
                    20, // Num containers
                    20, // Concurrency
                    step), // Duration
                ReservationRequest.newInstance(
                    Resource.newInstance(1024, 1), // Capability
                    10, // Num containers
                    10, // Concurrency
                    step) }, // Duration
            ReservationRequestInterpreter.R_ORDER_NO_GAP, "u1");

    try {
      ReservationId reservationID =
          ReservationSystemTestUtil.getNewReservationId();
      agent.createReservation(reservationID, "u1", plan, rr1);
      fail();
    } catch (PlanningException e) {
    }

    assertTrue("Agent-based allocation should have failed", plan
        .getAllReservations().size() == numJobsInScenario);

  }

  @Test
  public void testOrderImpossible() throws PlanningException {

    int numJobsInScenario = initializeScenario2();

    ReservationDefinition rr1 =
        createReservationDefinition(
            10 * step, // Job arrival time
            15 * step, // Job deadline
            new ReservationRequest[] {
                ReservationRequest.newInstance(
                    Resource.newInstance(1024, 1), // Capability
                    20, // Num containers
                    20, // Concurrency
                    2 * step), // Duration
                ReservationRequest.newInstance(
                    Resource.newInstance(1024, 1), // Capability
                    20, // Num containers
                    20, // Concurrency
                    step) }, // Duration
            ReservationRequestInterpreter.R_ORDER, "u1");

    try {
      ReservationId reservationID =
          ReservationSystemTestUtil.getNewReservationId();
      agent.createReservation(reservationID, "u1", plan, rr1);
      fail();
    } catch (PlanningException e) {
    }

    assertTrue("Agent-based allocation should have failed", plan
        .getAllReservations().size() == numJobsInScenario);

  }

  @Test
  public void testAnyImpossible() throws PlanningException {

    int numJobsInScenario = initializeScenario2();

    ReservationDefinition rr1 =
        createReservationDefinition(
            10 * step, // Job arrival time
            15 * step, // Job deadline
            new ReservationRequest[] {
                ReservationRequest.newInstance(
                    Resource.newInstance(1024, 1), // Capability
                    20, // Num containers
                    20, // Concurrency
                    3 * step), // Duration
                ReservationRequest.newInstance(
                    Resource.newInstance(1024, 1), // Capability
                    20, // Num containers
                    20, // Concurrency
                    2 * step) }, // Duration
            ReservationRequestInterpreter.R_ANY, "u1");

    try {
      ReservationId reservationID =
          ReservationSystemTestUtil.getNewReservationId();
      agent.createReservation(reservationID, "u1", plan, rr1);
      fail();
    } catch (PlanningException e) {
    }

    assertTrue("Agent-based allocation should have failed", plan
        .getAllReservations().size() == numJobsInScenario);

  }

  @Test
  public void testAnyAccept() throws PlanningException {

    int numJobsInScenario = initializeScenario2();

    ReservationDefinition rr1 =
        createReservationDefinition(
            10 * step, // Job arrival time
            15 * step, // Job deadline
            new ReservationRequest[] {
                ReservationRequest.newInstance(
                    Resource.newInstance(1024, 1), // Capability
                    20, // Num containers
                    20, // Concurrency
                    step), // Duration
                ReservationRequest.newInstance(
                    Resource.newInstance(1024, 1), // Capability
                    20, // Num containers
                    20, // Concurrency
                    2 * step) }, // Duration
            ReservationRequestInterpreter.R_ANY, "u1");

    ReservationId reservationID =
        ReservationSystemTestUtil.getNewReservationId();
    agent.createReservation(reservationID, "u1", plan, rr1);

    assertTrue("Agent-based allocation failed", reservationID != null);
    assertTrue("Agent-based allocation failed", plan.getAllReservations()
        .size() == numJobsInScenario + 1);

    ReservationAllocation alloc1 = plan.getReservationById(reservationID);

    assertTrue(alloc1.toString(),
        check(alloc1, 14 * step, 15 * step, 20, 1024, 1));

  }

  @Test
  public void testAllAccept() throws PlanningException {

    int numJobsInScenario = initializeScenario2();

    ReservationDefinition rr1 =
        createReservationDefinition(
            10 * step, // Job arrival time
            15 * step, // Job deadline
            new ReservationRequest[] {
                ReservationRequest.newInstance(
                    Resource.newInstance(1024, 1), // Capability
                    20, // Num containers
                    20, // Concurrency
                    step), // Duration
                ReservationRequest.newInstance(
                    Resource.newInstance(1024, 1), // Capability
                    20, // Num containers
                    20, // Concurrency
                    step) }, // Duration
            ReservationRequestInterpreter.R_ALL, "u1");

    ReservationId reservationID =
        ReservationSystemTestUtil.getNewReservationId();
    agent.createReservation(reservationID, "u1", plan, rr1);

    assertTrue("Agent-based allocation failed", reservationID != null);
    assertTrue("Agent-based allocation failed", plan.getAllReservations()
        .size() == numJobsInScenario + 1);

    ReservationAllocation alloc1 = plan.getReservationById(reservationID);

    assertTrue(alloc1.toString(),
        check(alloc1, 10 * step, 11 * step, 20, 1024, 1));
    assertTrue(alloc1.toString(),
        check(alloc1, 14 * step, 15 * step, 20, 1024, 1));

  }

  @Test
  public void testAllImpossible() throws PlanningException {

    int numJobsInScenario = initializeScenario2();

    ReservationDefinition rr1 =
        createReservationDefinition(
            10 * step, // Job arrival time
            15 * step, // Job deadline
            new ReservationRequest[] {
                ReservationRequest.newInstance(
                    Resource.newInstance(1024, 1), // Capability
                    20, // Num containers
                    20, // Concurrency
                    step), // Duration
                ReservationRequest.newInstance(
                    Resource.newInstance(1024, 1), // Capability
                    20, // Num containers
                    20, // Concurrency
                    2 * step) }, // Duration
            ReservationRequestInterpreter.R_ALL, "u1");

    try {
      ReservationId reservationID =
          ReservationSystemTestUtil.getNewReservationId();
      agent.createReservation(reservationID, "u1", plan, rr1);
      fail();
    } catch (PlanningException e) {
    }

    assertTrue("Agent-based allocation should have failed", plan
        .getAllReservations().size() == numJobsInScenario);

  }

  @Test
  public void testUpdate() throws PlanningException {

    ReservationDefinition rrFlex =
        createReservationDefinition(
            10 * step, // Job arrival time
            14 * step, // Job deadline
            new ReservationRequest[] { ReservationRequest.newInstance(
                Resource.newInstance(1024, 1), // Capability
                100, // Num containers
                1, // Concurrency
                2 * step) }, // Duration
            ReservationRequestInterpreter.R_ALL, "u1");

    ReservationDefinition rrBlock =
        createReservationDefinition(
            10 * step, // Job arrival time
            11 * step, // Job deadline
            new ReservationRequest[] { ReservationRequest.newInstance(
                Resource.newInstance(1024, 1), // Capability
                100, // Num containers
                100, // Concurrency
                step) }, // Duration
            ReservationRequestInterpreter.R_ALL, "u1");

    ReservationId flexReservationID =
        ReservationSystemTestUtil.getNewReservationId();
    ReservationId blockReservationID =
        ReservationSystemTestUtil.getNewReservationId();

    agent.createReservation(blockReservationID, "uBlock", plan, rrBlock);
    agent.createReservation(flexReservationID, "uFlex", plan, rrFlex);
    agent.deleteReservation(blockReservationID, "uBlock", plan);
    agent.updateReservation(flexReservationID, "uFlex", plan, rrFlex);

    assertTrue("Agent-based allocation failed", flexReservationID != null);
    assertTrue("Agent-based allocation failed", plan.getAllReservations()
        .size() == 1);

    ReservationAllocation alloc1 = plan.getReservationById(flexReservationID);

    assertTrue(alloc1.toString(),
        check(alloc1, 10 * step, 14 * step, 50, 1024, 1));

  }

  @Test
  public void testImpossibleDuration() throws PlanningException {

    ReservationDefinition rr1 =
        createReservationDefinition(
            10 * step, // Job arrival time
            15 * step, // Job deadline
            new ReservationRequest[] { ReservationRequest.newInstance(
                Resource.newInstance(1024, 1), // Capability
                20, // Num containers
                20, // Concurrency
                10 * step) }, // Duration
            ReservationRequestInterpreter.R_ALL, "u1");

    try {
      ReservationId reservationID =
          ReservationSystemTestUtil.getNewReservationId();
      agent.createReservation(reservationID, "u1", plan, rr1);
      fail();
    } catch (PlanningException e) {
    }

    assertTrue("Agent-based allocation should have failed", plan
        .getAllReservations().size() == 0);

  }

  @Test
  public void testLoadedDurationIntervals() throws PlanningException {

    int numJobsInScenario = initializeScenario3();

    ReservationDefinition rr1 =
        createReservationDefinition(
            10 * step, // Job arrival time
            13 * step, // Job deadline
            new ReservationRequest[] { ReservationRequest.newInstance(
                Resource.newInstance(1024, 1), // Capability
                80, // Num containers
                10, // Concurrency
                step) }, // Duration
            ReservationRequestInterpreter.R_ALL, "u1");

    ReservationId reservationID =
        ReservationSystemTestUtil.getNewReservationId();
    agent.createReservation(reservationID, "u1", plan, rr1);

    assertTrue("Agent-based allocation failed", reservationID != null);
    assertTrue("Agent-based allocation failed", plan.getAllReservations()
        .size() == numJobsInScenario + 1);

    ReservationAllocation alloc1 = plan.getReservationById(reservationID);

    assertTrue(alloc1.toString(),
        check(alloc1, 10 * step, 11 * step, 20, 1024, 1));
    assertTrue(alloc1.toString(),
        check(alloc1, 11 * step, 12 * step, 20, 1024, 1));
    assertTrue(alloc1.toString(),
        check(alloc1, 12 * step, 13 * step, 40, 1024, 1));
  }

  @Test
  public void testCostFunction() throws PlanningException {

    ReservationDefinition rr7Mem1Core =
        createReservationDefinition(
            10 * step, // Job arrival time
            11 * step, // Job deadline
            new ReservationRequest[] { ReservationRequest.newInstance(
                Resource.newInstance(7 * 1024, 1),// Capability
                1, // Num containers
                1, // Concurrency
                step) }, // Duration
            ReservationRequestInterpreter.R_ALL, "u1");

    ReservationDefinition rr6Mem6Cores =
        createReservationDefinition(
            10 * step, // Job arrival time
            11 * step, // Job deadline
            new ReservationRequest[] { ReservationRequest.newInstance(
                Resource.newInstance(6 * 1024, 6),// Capability
                1, // Num containers
                1, // Concurrency
                step) }, // Duration
            ReservationRequestInterpreter.R_ALL, "u2");

    ReservationDefinition rr =
        createReservationDefinition(
            10 * step, // Job arrival time
            12 * step, // Job deadline
            new ReservationRequest[] { ReservationRequest.newInstance(
                Resource.newInstance(1024, 1), // Capability
                1, // Num containers
                1, // Concurrency
                step) }, // Duration
            ReservationRequestInterpreter.R_ALL, "u3");

    ReservationId reservationID1 =
        ReservationSystemTestUtil.getNewReservationId();
    ReservationId reservationID2 =
        ReservationSystemTestUtil.getNewReservationId();
    ReservationId reservationID3 =
        ReservationSystemTestUtil.getNewReservationId();

    agent.createReservation(reservationID1, "u1", plan, rr7Mem1Core);
    agent.createReservation(reservationID2, "u2", plan, rr6Mem6Cores);
    agent.createReservation(reservationID3, "u3", plan, rr);

    ReservationAllocation alloc3 = plan.getReservationById(reservationID3);

    assertTrue(alloc3.toString(),
        check(alloc3, 10 * step, 11 * step, 0, 1024, 1));
    assertTrue(alloc3.toString(),
        check(alloc3, 11 * step, 12 * step, 1, 1024, 1));

  }

  @Test
  public void testFromCluster() throws PlanningException {


    List<ReservationDefinition> list = new ArrayList<ReservationDefinition>();

    list.add(createReservationDefinition(
        1425716392178L, // Job arrival time
        1425722262791L, // Job deadline
        new ReservationRequest[] { ReservationRequest.newInstance(
            Resource.newInstance(1024, 1), // Capability
            7, // Num containers
            1, // Concurrency
            587000) }, // Duration
        ReservationRequestInterpreter.R_ALL, "u1"));

    list.add(createReservationDefinition(
        1425716406178L, // Job arrival time
        1425721255841L, // Job deadline
        new ReservationRequest[] { ReservationRequest.newInstance(
            Resource.newInstance(1024, 1), // Capability
            6, // Num containers
            1, // Concurrency
            485000) }, // Duration
        ReservationRequestInterpreter.R_ALL, "u2"));

    list.add(createReservationDefinition(
        1425716399178L, // Job arrival time
        1425723780138L, // Job deadline
        new ReservationRequest[] { ReservationRequest.newInstance(
            Resource.newInstance(1024, 1), // Capability
            6, // Num containers
            1, // Concurrency
            738000) }, // Duration
        ReservationRequestInterpreter.R_ALL, "u3"));

    list.add(createReservationDefinition(
        1425716437178L, // Job arrival time
        1425722968378L, // Job deadline
        new ReservationRequest[] { ReservationRequest.newInstance(
            Resource.newInstance(1024, 1), // Capability
            7, // Num containers
            1, // Concurrency
            653000) }, // Duration
        ReservationRequestInterpreter.R_ALL, "u4"));

    list.add(createReservationDefinition(
        1425716406178L, // Job arrival time
        1425721926090L, // Job deadline
        new ReservationRequest[] { ReservationRequest.newInstance(
            Resource.newInstance(1024, 1), // Capability
            6, // Num containers
            1, // Concurrency
            552000) }, // Duration
        ReservationRequestInterpreter.R_ALL, "u5"));

    list.add(createReservationDefinition(
        1425716379178L, // Job arrival time
        1425722238553L, // Job deadline
        new ReservationRequest[] { ReservationRequest.newInstance(
            Resource.newInstance(1024, 1), // Capability
            6, // Num containers
            1, // Concurrency
            586000) }, // Duration
        ReservationRequestInterpreter.R_ALL, "u6"));

    list.add(createReservationDefinition(
        1425716407178L, // Job arrival time
        1425722908317L, // Job deadline
        new ReservationRequest[] { ReservationRequest.newInstance(
            Resource.newInstance(1024, 1), // Capability
            7, // Num containers
            1, // Concurrency
            650000) }, // Duration
        ReservationRequestInterpreter.R_ALL, "u7"));

    list.add(createReservationDefinition(
        1425716452178L, // Job arrival time
        1425722841562L, // Job deadline
        new ReservationRequest[] { ReservationRequest.newInstance(
            Resource.newInstance(1024, 1), // Capability
            6, // Num containers
            1, // Concurrency
            639000) }, // Duration
        ReservationRequestInterpreter.R_ALL, "u8"));

    list.add(createReservationDefinition(
        1425716384178L, // Job arrival time
        1425721766129L, // Job deadline
        new ReservationRequest[] { ReservationRequest.newInstance(
            Resource.newInstance(1024, 1), // Capability
            7, // Num containers
            1, // Concurrency
            538000) }, // Duration
        ReservationRequestInterpreter.R_ALL, "u9"));

    list.add(createReservationDefinition(
        1425716437178L, // Job arrival time
        1425722507886L, // Job deadline
        new ReservationRequest[] { ReservationRequest.newInstance(
            Resource.newInstance(1024, 1), // Capability
            5, // Num containers
            1, // Concurrency
            607000) }, // Duration
        ReservationRequestInterpreter.R_ALL, "u10"));

    int i = 1;
    for (ReservationDefinition rr : list) {
      ReservationId reservationID =
          ReservationSystemTestUtil.getNewReservationId();
      agent.createReservation(reservationID, "u" + Integer.toString(i), plan,
          rr);
      ++i;
    }

    assertTrue("Agent-based allocation failed", plan.getAllReservations()
        .size() == list.size());

  }

  @Before
  public void setup() throws Exception {

    long seed = rand.nextLong();
    rand.setSeed(seed);
    Log.info("Running with seed: " + seed);

    long timeWindow = 1000000L;
    int capacityMem = 100 * 1024;
    int capacityCores = 100;
    step = 60000L;

    Resource clusterCapacity = Resource.newInstance(capacityMem, capacityCores);

    ReservationSystemTestUtil testUtil = new ReservationSystemTestUtil();
    String reservationQ = testUtil.getFullReservationQueueName();
    float instConstraint = 100;
    float avgConstraint = 100;

    ReservationSchedulerConfiguration conf =
        ReservationSystemTestUtil.createConf(reservationQ, timeWindow,
            instConstraint, avgConstraint);

    CapacityOverTimePolicy policy = new CapacityOverTimePolicy();
    policy.init(reservationQ, conf);

    QueueMetrics queueMetrics = mock(QueueMetrics.class);

    agent = new AlignedPlannerWithGreedy();

    plan =
        new InMemoryPlan(queueMetrics, policy, agent, clusterCapacity, step,
            res, minAlloc, maxAlloc, "dedicated", null, true);
  }

  private int initializeScenario1() throws PlanningException {


    addFixedAllocation(0L, step, new int[] { 10, 10, 20, 20, 20, 10, 10 });

    System.out.println("--------BEFORE AGENT----------");
    System.out.println(plan.toString());
    System.out.println(plan.toCumulativeString());

    return 1;

  }

  private int initializeScenario2() throws PlanningException {


    addFixedAllocation(11 * step, step, new int[] { 90, 90, 90 });

    System.out.println("--------BEFORE AGENT----------");
    System.out.println(plan.toString());
    System.out.println(plan.toCumulativeString());

    return 1;

  }

  private int initializeScenario3() throws PlanningException {


    addFixedAllocation(10 * step, step, new int[] { 70, 80, 60 });

    System.out.println("--------BEFORE AGENT----------");
    System.out.println(plan.toString());
    System.out.println(plan.toCumulativeString());

    return 1;

  }

  private void addFixedAllocation(long start, long step, int[] f)
      throws PlanningException {

    assertTrue(plan.toString(),
        plan.addReservation(new InMemoryReservationAllocation(
            ReservationSystemTestUtil.getNewReservationId(), null,
            "user_fixed", "dedicated", start, start + f.length * step,
            ReservationSystemTestUtil.generateAllocation(start, step, f), res,
            minAlloc)));

  }

  private ReservationDefinition createReservationDefinition(long arrival,
      long deadline, ReservationRequest[] reservationRequests,
      ReservationRequestInterpreter rType, String username) {

    return ReservationDefinition.newInstance(arrival, deadline,
        ReservationRequests.newInstance(Arrays.asList(reservationRequests),
            rType), username);

  }

  private boolean check(ReservationAllocation alloc, long start, long end,
      int containers, int mem, int cores) {

    Resource expectedResources =
        Resource.newInstance(mem * containers, cores * containers);

    for (long i = start; i < end; i++) {
      if (!Resources.equals(alloc.getResourcesAtTime(i), expectedResources)) {
        return false;
      }
    }
    return true;

  }

}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/reservation/planning/TestGreedyReservationAgent.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/reservation/planning/TestGreedyReservationAgent.java
package org.apache.hadoop.yarn.server.resourcemanager.reservation.planning;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.yarn.api.records.ReservationDefinition;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.ReservationRequest;
import org.apache.hadoop.yarn.api.records.ReservationRequestInterpreter;
import org.apache.hadoop.yarn.api.records.ReservationRequests;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.impl.pb.ReservationDefinitionPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ReservationRequestsPBImpl;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.CapacityOverTimePolicy;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.InMemoryPlan;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.InMemoryReservationAllocation;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationAllocation;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationInterval;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationSchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationSystemTestUtil;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions.PlanningException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.Before;
import org.junit.Test;
import org.mortbay.log.Log;

public class TestGreedyReservationAgent {

  ReservationAgent agent;
  InMemoryPlan plan;
  Resource minAlloc = Resource.newInstance(1024, 1);
  ResourceCalculator res = new DefaultResourceCalculator();
  Resource maxAlloc = Resource.newInstance(1024 * 8, 8);
  Random rand = new Random();
  long step;

  @Before
  public void setup() throws Exception {

    long seed = rand.nextLong();
    rand.setSeed(seed);
    Log.info("Running with seed: " + seed);

    long timeWindow = 1000000L;
    Resource clusterCapacity = Resource.newInstance(100 * 1024, 100);
    step = 1000L;
    ReservationSystemTestUtil testUtil = new ReservationSystemTestUtil();
    String reservationQ = testUtil.getFullReservationQueueName();

    float instConstraint = 100;
    float avgConstraint = 100;

    ReservationSchedulerConfiguration conf =
        ReservationSystemTestUtil.createConf(reservationQ, timeWindow,
            instConstraint, avgConstraint);
    CapacityOverTimePolicy policy = new CapacityOverTimePolicy();
    policy.init(reservationQ, conf);
    agent = new GreedyReservationAgent();

    QueueMetrics queueMetrics = mock(QueueMetrics.class);

    plan = new InMemoryPlan(queueMetrics, policy, agent, clusterCapacity, step,
        res, minAlloc, maxAlloc, "dedicated", null, true);
  }

  @SuppressWarnings("javadoc")
  @Test
  public void testSimple() throws PlanningException {

    prepareBasicPlan();

    ReservationDefinition rr = new ReservationDefinitionPBImpl();
    rr.setArrival(5 * step);
    rr.setDeadline(20 * step);
    ReservationRequest r = ReservationRequest.newInstance(
        Resource.newInstance(2048, 2), 10, 5, 10 * step);
    ReservationRequests reqs = new ReservationRequestsPBImpl();
    reqs.setReservationResources(Collections.singletonList(r));
    rr.setReservationRequests(reqs);

    ReservationId reservationID = ReservationSystemTestUtil
        .getNewReservationId();
    agent.createReservation(reservationID, "u1", plan, rr);

    assertTrue("Agent-based allocation failed", reservationID != null);
    assertTrue("Agent-based allocation failed", plan.getAllReservations()
        .size() == 3);

    ReservationAllocation cs = plan.getReservationById(reservationID);

    System.out.println("--------AFTER SIMPLE ALLOCATION (queue: "
        + reservationID + ")----------");
    System.out.println(plan.toString());
    System.out.println(plan.toCumulativeString());

    for (long i = 10 * step; i < 20 * step; i++) {
      assertTrue(
          "Agent-based allocation unexpected",
          Resources.equals(cs.getResourcesAtTime(i),
              Resource.newInstance(2048 * 10, 2 * 10)));
    }

  }

  @Test
  public void testOrder() throws PlanningException {
    prepareBasicPlan();

    int[] f = { 100, 100 };

    assertTrue(plan.toString(),
        plan.addReservation(new InMemoryReservationAllocation(
            ReservationSystemTestUtil.getNewReservationId(), null, "u1",
            "dedicated", 30 * step, 30 * step + f.length * step,
            ReservationSystemTestUtil.generateAllocation(30 * step, step, f),
            res, minAlloc)));

    ReservationDefinition rr = new ReservationDefinitionPBImpl();
    rr.setArrival(0 * step);
    rr.setDeadline(70 * step);
    ReservationRequests reqs = new ReservationRequestsPBImpl();
    reqs.setInterpreter(ReservationRequestInterpreter.R_ORDER);
    ReservationRequest r = ReservationRequest.newInstance(
        Resource.newInstance(2048, 2), 10, 1, 10 * step);
    ReservationRequest r2 = ReservationRequest.newInstance(
        Resource.newInstance(1024, 1), 10, 10, 20 * step);
    List<ReservationRequest> list = new ArrayList<ReservationRequest>();
    list.add(r);
    list.add(r2);
    list.add(r);
    list.add(r2);
    reqs.setReservationResources(list);
    rr.setReservationRequests(reqs);

    ReservationId reservationID = ReservationSystemTestUtil
        .getNewReservationId();
    agent.createReservation(reservationID, "u1", plan, rr);

    assertTrue("Agent-based allocation failed", reservationID != null);
    assertTrue("Agent-based allocation failed", plan.getAllReservations()
        .size() == 4);

    ReservationAllocation cs = plan.getReservationById(reservationID);

    assertTrue(cs.toString(), check(cs, 0 * step, 10 * step, 20, 1024, 1));
    assertTrue(cs.toString(), check(cs, 10 * step, 30 * step, 10, 1024, 1));
    assertTrue(cs.toString(), check(cs, 40 * step, 50 * step, 20, 1024, 1));
    assertTrue(cs.toString(), check(cs, 50 * step, 70 * step, 10, 1024, 1));

    System.out.println("--------AFTER ORDER ALLOCATION (queue: "
        + reservationID + ")----------");
    System.out.println(plan.toString());
    System.out.println(plan.toCumulativeString());

  }

  @Test
  public void testOrderNoGapImpossible() throws PlanningException {
    prepareBasicPlan();
    int[] f = { 100, 100 };

    assertTrue(plan.toString(),
        plan.addReservation(new InMemoryReservationAllocation(
            ReservationSystemTestUtil.getNewReservationId(), null, "u1",
            "dedicated", 30 * step, 30 * step + f.length * step,
            ReservationSystemTestUtil.generateAllocation(30 * step, step, f),
            res, minAlloc)));

    ReservationDefinition rr = new ReservationDefinitionPBImpl();
    rr.setArrival(0L);

    rr.setDeadline(70L);
    ReservationRequests reqs = new ReservationRequestsPBImpl();
    reqs.setInterpreter(ReservationRequestInterpreter.R_ORDER_NO_GAP);
    ReservationRequest r = ReservationRequest.newInstance(
        Resource.newInstance(2048, 2), 10, 1, 10);
    ReservationRequest r2 = ReservationRequest.newInstance(
        Resource.newInstance(1024, 1), 10, 10, 20);
    List<ReservationRequest> list = new ArrayList<ReservationRequest>();
    list.add(r);
    list.add(r2);
    list.add(r);
    list.add(r2);
    reqs.setReservationResources(list);
    rr.setReservationRequests(reqs);

    ReservationId reservationID = ReservationSystemTestUtil
        .getNewReservationId();
    boolean result = false;
    try {
      result = agent.createReservation(reservationID, "u1", plan, rr);
      fail();
    } catch (PlanningException p) {
    }

    assertFalse("Agent-based allocation should have failed", result);
    assertTrue("Agent-based allocation should have failed", plan
        .getAllReservations().size() == 3);

    System.out
        .println("--------AFTER ORDER_NO_GAP IMPOSSIBLE ALLOCATION (queue: "
            + reservationID + ")----------");
    System.out.println(plan.toString());
    System.out.println(plan.toCumulativeString());

  }

  @Test
  public void testOrderNoGap() throws PlanningException {
    prepareBasicPlan();
    ReservationDefinition rr = new ReservationDefinitionPBImpl();
    rr.setArrival(0 * step);
    rr.setDeadline(60 * step);
    ReservationRequests reqs = new ReservationRequestsPBImpl();
    reqs.setInterpreter(ReservationRequestInterpreter.R_ORDER_NO_GAP);
    ReservationRequest r = ReservationRequest.newInstance(
        Resource.newInstance(2048, 2), 10, 1, 10 * step);
    ReservationRequest r2 = ReservationRequest.newInstance(
        Resource.newInstance(1024, 1), 10, 10, 20 * step);
    List<ReservationRequest> list = new ArrayList<ReservationRequest>();
    list.add(r);
    list.add(r2);
    list.add(r);
    list.add(r2);
    reqs.setReservationResources(list);
    rr.setReservationRequests(reqs);
    rr.setReservationRequests(reqs);

    ReservationId reservationID = ReservationSystemTestUtil
        .getNewReservationId();
    agent.createReservation(reservationID, "u1", plan, rr);

    System.out.println("--------AFTER ORDER ALLOCATION (queue: "
        + reservationID + ")----------");
    System.out.println(plan.toString());
    System.out.println(plan.toCumulativeString());

    assertTrue("Agent-based allocation failed", reservationID != null);
    assertTrue("Agent-based allocation failed", plan.getAllReservations()
        .size() == 3);

    ReservationAllocation cs = plan.getReservationById(reservationID);

    assertTrue(cs.toString(), check(cs, 0 * step, 10 * step, 20, 1024, 1));
    assertTrue(cs.toString(), check(cs, 10 * step, 30 * step, 10, 1024, 1));
    assertTrue(cs.toString(), check(cs, 30 * step, 40 * step, 20, 1024, 1));
    assertTrue(cs.toString(), check(cs, 40 * step, 60 * step, 10, 1024, 1));

  }

  @Test
  public void testSingleSliding() throws PlanningException {
    prepareBasicPlan();

    ReservationDefinition rr = new ReservationDefinitionPBImpl();
    rr.setArrival(100 * step);
    rr.setDeadline(120 * step);
    ReservationRequests reqs = new ReservationRequestsPBImpl();
    reqs.setInterpreter(ReservationRequestInterpreter.R_ALL);
    ReservationRequest r = ReservationRequest.newInstance(
        Resource.newInstance(1024, 1), 200, 10, 10 * step);

    List<ReservationRequest> list = new ArrayList<ReservationRequest>();
    list.add(r);
    reqs.setReservationResources(list);
    rr.setReservationRequests(reqs);

    ReservationId reservationID = ReservationSystemTestUtil
        .getNewReservationId();
    agent.createReservation(reservationID, "u1", plan, rr);

    assertTrue("Agent-based allocation failed", reservationID != null);
    assertTrue("Agent-based allocation failed", plan.getAllReservations()
        .size() == 3);

    ReservationAllocation cs = plan.getReservationById(reservationID);

    assertTrue(cs.toString(), check(cs, 100 * step, 120 * step, 100, 1024, 1));

    System.out.println("--------AFTER packed ALLOCATION (queue: "
        + reservationID + ")----------");
    System.out.println(plan.toString());
    System.out.println(plan.toCumulativeString());

  }

  @Test
  public void testAny() throws PlanningException {
    prepareBasicPlan();

    ReservationDefinition rr = new ReservationDefinitionPBImpl();
    rr.setArrival(100 * step);
    rr.setDeadline(120 * step);
    ReservationRequests reqs = new ReservationRequestsPBImpl();
    reqs.setInterpreter(ReservationRequestInterpreter.R_ANY);
    ReservationRequest r = ReservationRequest.newInstance(
        Resource.newInstance(1024, 1), 5, 5, 10 * step);
    ReservationRequest r2 = ReservationRequest.newInstance(
        Resource.newInstance(2048, 2), 10, 5, 10 * step);
    ReservationRequest r3 = ReservationRequest.newInstance(
        Resource.newInstance(1024, 1), 110, 110, 10 * step);

    List<ReservationRequest> list = new ArrayList<ReservationRequest>();
    list.add(r);
    list.add(r2);
    list.add(r3);
    reqs.setReservationResources(list);
    rr.setReservationRequests(reqs);

    ReservationId reservationID = ReservationSystemTestUtil
        .getNewReservationId();
    boolean res = agent.createReservation(reservationID, "u1", plan, rr);

    assertTrue("Agent-based allocation failed", res);
    assertTrue("Agent-based allocation failed", plan.getAllReservations()
        .size() == 3);

    ReservationAllocation cs = plan.getReservationById(reservationID);

    assertTrue(cs.toString(), check(cs, 110 * step, 120 * step, 20, 1024, 1));

    System.out.println("--------AFTER ANY ALLOCATION (queue: " + reservationID
        + ")----------");
    System.out.println(plan.toString());
    System.out.println(plan.toCumulativeString());

  }

  @Test
  public void testAnyImpossible() throws PlanningException {
    prepareBasicPlan();
    ReservationDefinition rr = new ReservationDefinitionPBImpl();
    rr.setArrival(100L);
    rr.setDeadline(120L);
    ReservationRequests reqs = new ReservationRequestsPBImpl();
    reqs.setInterpreter(ReservationRequestInterpreter.R_ANY);

    ReservationRequest r1 = ReservationRequest.newInstance(
        Resource.newInstance(1024, 1), 35, 5, 30);
    ReservationRequest r2 = ReservationRequest.newInstance(
        Resource.newInstance(1024, 1), 110, 110, 10);

    List<ReservationRequest> list = new ArrayList<ReservationRequest>();
    list.add(r1);
    list.add(r2);
    reqs.setReservationResources(list);
    rr.setReservationRequests(reqs);

    ReservationId reservationID = ReservationSystemTestUtil
        .getNewReservationId();
    boolean result = false;
    try {
      result = agent.createReservation(reservationID, "u1", plan, rr);
      fail();
    } catch (PlanningException p) {
    }
    assertFalse("Agent-based allocation should have failed", result);
    assertTrue("Agent-based allocation should have failed", plan
        .getAllReservations().size() == 2);

    System.out.println("--------AFTER ANY IMPOSSIBLE ALLOCATION (queue: "
        + reservationID + ")----------");
    System.out.println(plan.toString());
    System.out.println(plan.toCumulativeString());

  }

  @Test
  public void testAll() throws PlanningException {
    prepareBasicPlan();
    ReservationDefinition rr = new ReservationDefinitionPBImpl();
    rr.setArrival(100 * step);
    rr.setDeadline(120 * step);
    ReservationRequests reqs = new ReservationRequestsPBImpl();
    reqs.setInterpreter(ReservationRequestInterpreter.R_ALL);
    ReservationRequest r = ReservationRequest.newInstance(
        Resource.newInstance(1024, 1), 5, 5, 10 * step);
    ReservationRequest r2 = ReservationRequest.newInstance(
        Resource.newInstance(2048, 2), 10, 10, 20 * step);

    List<ReservationRequest> list = new ArrayList<ReservationRequest>();
    list.add(r);
    list.add(r2);
    reqs.setReservationResources(list);
    rr.setReservationRequests(reqs);

    ReservationId reservationID = ReservationSystemTestUtil
        .getNewReservationId();
    agent.createReservation(reservationID, "u1", plan, rr);

    assertTrue("Agent-based allocation failed", reservationID != null);
    assertTrue("Agent-based allocation failed", plan.getAllReservations()
        .size() == 3);

    ReservationAllocation cs = plan.getReservationById(reservationID);

    assertTrue(cs.toString(), check(cs, 100 * step, 110 * step, 20, 1024, 1));
    assertTrue(cs.toString(), check(cs, 110 * step, 120 * step, 25, 1024, 1));

    System.out.println("--------AFTER ALL ALLOCATION (queue: " + reservationID
        + ")----------");
    System.out.println(plan.toString());
    System.out.println(plan.toCumulativeString());

  }

  @Test
  public void testAllImpossible() throws PlanningException {
    prepareBasicPlan();
    ReservationDefinition rr = new ReservationDefinitionPBImpl();
    rr.setArrival(100L);
    rr.setDeadline(120L);
    ReservationRequests reqs = new ReservationRequestsPBImpl();
    reqs.setInterpreter(ReservationRequestInterpreter.R_ALL);
    ReservationRequest r = ReservationRequest.newInstance(
        Resource.newInstance(1024, 1), 55, 5, 10);
    ReservationRequest r2 = ReservationRequest.newInstance(
        Resource.newInstance(2048, 2), 55, 5, 20);

    List<ReservationRequest> list = new ArrayList<ReservationRequest>();
    list.add(r);
    list.add(r2);
    reqs.setReservationResources(list);
    rr.setReservationRequests(reqs);

    ReservationId reservationID = ReservationSystemTestUtil
        .getNewReservationId();
    boolean result = false;
    try {
      result = agent.createReservation(reservationID, "u1", plan, rr);
      fail();
    } catch (PlanningException p) {
    }

    assertFalse("Agent-based allocation failed", result);
    assertTrue("Agent-based allocation failed", plan.getAllReservations()
        .size() == 2);

    System.out.println("--------AFTER ALL IMPOSSIBLE ALLOCATION (queue: "
        + reservationID + ")----------");
    System.out.println(plan.toString());
    System.out.println(plan.toCumulativeString());

  }

  private void prepareBasicPlan() throws PlanningException {


    int[] f = { 10, 10, 20, 20, 20, 10, 10 };

    assertTrue(plan.toString(),
        plan.addReservation(new InMemoryReservationAllocation(
            ReservationSystemTestUtil.getNewReservationId(), null, "u1",
            "dedicated", 0L, 0L + f.length * step, ReservationSystemTestUtil
                .generateAllocation(0, step, f), res, minAlloc)));

    int[] f2 = { 5, 5, 5, 5, 5, 5, 5 };
    Map<ReservationInterval, Resource> alloc =
        ReservationSystemTestUtil.generateAllocation(5000, step, f2);
    assertTrue(plan.toString(),
        plan.addReservation(new InMemoryReservationAllocation(
            ReservationSystemTestUtil.getNewReservationId(), null, "u1",
            "dedicated", 5000, 5000 + f2.length * step, alloc, res, minAlloc)));

    System.out.println("--------BEFORE AGENT----------");
    System.out.println(plan.toString());
    System.out.println(plan.toCumulativeString());
  }

  private boolean check(ReservationAllocation cs, long start, long end,
      int containers, int mem, int cores) {

    boolean res = true;
    for (long i = start; i < end; i++) {
      res = res
          && Resources.equals(cs.getResourcesAtTime(i),
              Resource.newInstance(mem * containers, cores * containers));
    }
    return res;
  }

  public void testStress(int numJobs) throws PlanningException, IOException {

    long timeWindow = 1000000L;
    Resource clusterCapacity = Resource.newInstance(500 * 100 * 1024, 500 * 32);
    step = 1000L;
    ReservationSystemTestUtil testUtil = new ReservationSystemTestUtil();
    CapacityScheduler scheduler = testUtil.mockCapacityScheduler(500 * 100);
    String reservationQ = testUtil.getFullReservationQueueName();
    float instConstraint = 100;
    float avgConstraint = 100;
    ReservationSchedulerConfiguration conf =
        ReservationSystemTestUtil.createConf(reservationQ, timeWindow,
            instConstraint, avgConstraint);
    CapacityOverTimePolicy policy = new CapacityOverTimePolicy();
    policy.init(reservationQ, conf);

    plan = new InMemoryPlan(scheduler.getRootQueueMetrics(), policy, agent,
      clusterCapacity, step, res, minAlloc, maxAlloc, "dedicated", null, true);

    int acc = 0;
    List<ReservationDefinition> list = new ArrayList<ReservationDefinition>();
    for (long i = 0; i < numJobs; i++) {
      list.add(ReservationSystemTestUtil.generateRandomRR(rand, i));
    }

    long start = System.currentTimeMillis();
    for (int i = 0; i < numJobs; i++) {

      try {
        if (agent.createReservation(
            ReservationSystemTestUtil.getNewReservationId(), "u" + i % 100,
            plan, list.get(i))) {
          acc++;
        }
      } catch (PlanningException p) {
      }
    }

    long end = System.currentTimeMillis();
    System.out.println("Submitted " + numJobs + " jobs " + " accepted " + acc
        + " in " + (end - start) + "ms");
  }

  public static void main(String[] arg) {

    int numJobs = 1000;
    if (arg.length > 0) {
      numJobs = Integer.parseInt(arg[0]);
    }

    try {
      TestGreedyReservationAgent test = new TestGreedyReservationAgent();
      test.setup();
      test.testStress(numJobs);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/reservation/planning/TestSimpleCapacityReplanner.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/reservation/planning/TestSimpleCapacityReplanner.java
package org.apache.hadoop.yarn.server.resourcemanager.reservation.planning;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.ReservationRequest;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.InMemoryPlan;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.InMemoryReservationAllocation;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.NoOverCommitPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationAllocation;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationInterval;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationSchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationSystemUtil;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.SharingPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions.PlanningException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.junit.Test;

public class TestSimpleCapacityReplanner {

  @Test
  public void testReplanningPlanCapacityLoss() throws PlanningException {

    Resource clusterCapacity = Resource.newInstance(100 * 1024, 10);
    Resource minAlloc = Resource.newInstance(1024, 1);
    Resource maxAlloc = Resource.newInstance(1024 * 8, 8);

    ResourceCalculator res = new DefaultResourceCalculator();
    long step = 1L;
    Clock clock = mock(Clock.class);
    ReservationAgent agent = mock(ReservationAgent.class);

    SharingPolicy policy = new NoOverCommitPolicy();
    policy.init("root.dedicated", null);

    QueueMetrics queueMetrics = mock(QueueMetrics.class);

    when(clock.getTime()).thenReturn(0L);
    SimpleCapacityReplanner enf = new SimpleCapacityReplanner(clock);

    ReservationSchedulerConfiguration conf =
        mock(ReservationSchedulerConfiguration.class);
    when(conf.getEnforcementWindow(any(String.class))).thenReturn(6L);

    enf.init("blah", conf);

    InMemoryPlan plan =
        new InMemoryPlan(queueMetrics, policy, agent, clusterCapacity, step,
            res, minAlloc, maxAlloc, "dedicated", enf, true, clock);

    long ts = System.currentTimeMillis();
    ReservationId r1 = ReservationId.newInstance(ts, 1);
    int[] f5 = { 20, 20, 20, 20, 20 };
    assertTrue(plan.toString(),
        plan.addReservation(new InMemoryReservationAllocation(r1, null, "u3",
            "dedicated", 0, 0 + f5.length, generateAllocation(0, f5), res,
            minAlloc)));
    when(clock.getTime()).thenReturn(1L);
    ReservationId r2 = ReservationId.newInstance(ts, 2);
    assertTrue(plan.toString(),
        plan.addReservation(new InMemoryReservationAllocation(r2, null, "u4",
            "dedicated", 0, 0 + f5.length, generateAllocation(0, f5), res,
            minAlloc)));
    when(clock.getTime()).thenReturn(2L);
    ReservationId r3 = ReservationId.newInstance(ts, 3);
    assertTrue(plan.toString(),
        plan.addReservation(new InMemoryReservationAllocation(r3, null, "u5",
            "dedicated", 0, 0 + f5.length, generateAllocation(0, f5), res,
            minAlloc)));
    when(clock.getTime()).thenReturn(3L);
    ReservationId r4 = ReservationId.newInstance(ts, 4);
    assertTrue(plan.toString(),
        plan.addReservation(new InMemoryReservationAllocation(r4, null, "u6",
            "dedicated", 0, 0 + f5.length, generateAllocation(0, f5), res,
            minAlloc)));
    when(clock.getTime()).thenReturn(4L);
    ReservationId r5 = ReservationId.newInstance(ts, 5);
    assertTrue(plan.toString(),
        plan.addReservation(new InMemoryReservationAllocation(r5, null, "u7",
            "dedicated", 0, 0 + f5.length, generateAllocation(0, f5), res,
            minAlloc)));

    int[] f6 = { 50, 50, 50, 50, 50 };
    ReservationId r6 = ReservationId.newInstance(ts, 6);
    assertTrue(plan.toString(),
        plan.addReservation(new InMemoryReservationAllocation(r6, null, "u3",
            "dedicated", 10, 10 + f6.length, generateAllocation(10, f6), res,
            minAlloc)));
    when(clock.getTime()).thenReturn(6L);
    ReservationId r7 = ReservationId.newInstance(ts, 7);
    assertTrue(plan.toString(),
        plan.addReservation(new InMemoryReservationAllocation(r7, null, "u4",
            "dedicated", 10, 10 + f6.length, generateAllocation(10, f6), res,
            minAlloc)));

    plan.setTotalCapacity(Resource.newInstance(70 * 1024, 70));

    when(clock.getTime()).thenReturn(0L);

    enf.plan(plan, null);

    assertNotNull(plan.getReservationById(r1));
    assertNotNull(plan.getReservationById(r2));
    assertNotNull(plan.getReservationById(r3));
    assertNotNull(plan.getReservationById(r6));
    assertNotNull(plan.getReservationById(r7));

    assertNull(plan.getReservationById(r4));
    assertNull(plan.getReservationById(r5));

    for (int i = 0; i < 20; i++) {
      int tot = 0;
      for (ReservationAllocation r : plan.getReservationsAtTime(i)) {
        tot = r.getResourcesAtTime(i).getMemory();
      }
      assertTrue(tot <= 70 * 1024);
    }
  }

  private Map<ReservationInterval, Resource> generateAllocation(
      int startTime, int[] alloc) {
    Map<ReservationInterval, Resource> req =
        new TreeMap<ReservationInterval, Resource>();
    for (int i = 0; i < alloc.length; i++) {
      req.put(new ReservationInterval(startTime + i, startTime + i + 1),
          ReservationSystemUtil.toResource(
              ReservationRequest.newInstance(Resource.newInstance(1024, 1),
                  alloc[i])));
    }
    return req;
  }

}

