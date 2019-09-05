hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/reservation/GreedyReservationAgent.java
hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/reservation/InMemoryPlan.java
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions.PlanningException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;

  private void incrementAllocation(ReservationAllocation reservation) {
    assert (readWriteLock.isWriteLockedByCurrentThread());
    Map<ReservationInterval, Resource> allocationRequests =
        reservation.getAllocationRequests();
    String user = reservation.getUser();
      resAlloc = new RLESparseResourceAllocation(resCalc, minAlloc);
      userResourceAlloc.put(user, resAlloc);
    }
    for (Map.Entry<ReservationInterval, Resource> r : allocationRequests
        .entrySet()) {
      resAlloc.addInterval(r.getKey(), r.getValue());
      rleSparseVector.addInterval(r.getKey(), r.getValue());

  private void decrementAllocation(ReservationAllocation reservation) {
    assert (readWriteLock.isWriteLockedByCurrentThread());
    Map<ReservationInterval, Resource> allocationRequests =
        reservation.getAllocationRequests();
    String user = reservation.getUser();
    RLESparseResourceAllocation resAlloc = userResourceAlloc.get(user);
    for (Map.Entry<ReservationInterval, Resource> r : allocationRequests
        .entrySet()) {
      resAlloc.removeInterval(r.getKey(), r.getValue());
      rleSparseVector.removeInterval(r.getKey(), r.getValue());

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/reservation/InMemoryReservationAllocation.java

import org.apache.hadoop.yarn.api.records.ReservationDefinition;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;
  private final ReservationDefinition contract;
  private final long startTime;
  private final long endTime;
  private final Map<ReservationInterval, Resource> allocationRequests;
  private boolean hasGang = false;
  private long acceptedAt = -1;

  InMemoryReservationAllocation(ReservationId reservationID,
      ReservationDefinition contract, String user, String planName,
      long startTime, long endTime,
      Map<ReservationInterval, Resource> allocations,
      ResourceCalculator calculator, Resource minAlloc) {
    this(reservationID, contract, user, planName, startTime, endTime,
        allocations, calculator, minAlloc, false);
  }

  InMemoryReservationAllocation(ReservationId reservationID,
      ReservationDefinition contract, String user, String planName,
      long startTime, long endTime,
      Map<ReservationInterval, Resource> allocations,
      ResourceCalculator calculator, Resource minAlloc, boolean hasGang) {
    this.contract = contract;
    this.startTime = startTime;
    this.endTime = endTime;
    this.reservationID = reservationID;
    this.user = user;
    this.allocationRequests = allocations;
    this.planName = planName;
    this.hasGang = hasGang;
    resourcesOverTime = new RLESparseResourceAllocation(calculator, minAlloc);
    for (Map.Entry<ReservationInterval, Resource> r : allocations
        .entrySet()) {
      resourcesOverTime.addInterval(r.getKey(), r.getValue());
    }
  }

  }

  @Override
  public Map<ReservationInterval, Resource> getAllocationRequests() {
    return Collections.unmodifiableMap(allocationRequests);
  }


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/reservation/RLESparseResourceAllocation.java
import java.io.IOException;
import java.io.StringWriter;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

  public boolean addInterval(ReservationInterval reservationInterval,
      Resource totCap) {
    if (totCap.equals(ZERO_RESOURCE)) {
      return true;
    }
    }
  }

  public boolean removeInterval(ReservationInterval reservationInterval,
      Resource totCap) {
    if (totCap.equals(ZERO_RESOURCE)) {
      return true;
    }

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/reservation/ReservationAllocation.java

import org.apache.hadoop.yarn.api.records.ReservationDefinition;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.Resource;

  public Map<ReservationInterval, Resource> getAllocationRequests();


hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/reservation/ReservationSystemUtil.java
++ b/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/reservation/ReservationSystemUtil.java

package org.apache.hadoop.yarn.server.resourcemanager.reservation;

import org.apache.hadoop.yarn.api.records.ReservationRequest;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.resource.Resources;

import java.util.HashMap;
import java.util.Map;

final class ReservationSystemUtil {

  private ReservationSystemUtil() {
  }

  public static Resource toResource(ReservationRequest request) {
    Resource resource = Resources.multiply(request.getCapability(),
        (float) request.getNumContainers());
    return resource;
  }

  public static Map<ReservationInterval, Resource> toResources(
      Map<ReservationInterval, ReservationRequest> allocations) {
    Map<ReservationInterval, Resource> resources =
        new HashMap<ReservationInterval, Resource>();
    for (Map.Entry<ReservationInterval, ReservationRequest> entry :
        allocations.entrySet()) {
      resources.put(entry.getKey(),
          toResource(entry.getValue()));
    }
    return resources;
  }
}

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/reservation/ReservationSystemTestUtil.java
    return rr;
  }

  public static Map<ReservationInterval, Resource> generateAllocation(
      long startTime, long step, int[] alloc) {
    Map<ReservationInterval, Resource> req =
        new TreeMap<ReservationInterval, Resource>();
    for (int i = 0; i < alloc.length; i++) {
      req.put(new ReservationInterval(startTime + i * step, startTime + (i + 1)
          .newInstance(
          Resource.newInstance(1024, 1), alloc[i])));
    }
    return req;
  }

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/reservation/TestCapacityOverTimePolicy.java

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.util.Map;
  @Test(expected = PlanningQuotaException.class)
  public void testFailAvg() throws IOException, PlanningException {
    Map<ReservationInterval, Resource> req =
        new TreeMap<ReservationInterval, Resource>();
    long win = timeWindow / 2 + 100;
    int cont = (int) Math.ceil(0.5 * totCont);
    req.put(new ReservationInterval(initTime, initTime + win),
        ReservationSystemUtil.toResource(
            ReservationRequest.newInstance(Resource.newInstance(1024, 1),
                cont)));

    assertTrue(plan.toString(),
        plan.addReservation(new InMemoryReservationAllocation(
  @Test
  public void testFailAvgBySum() throws IOException, PlanningException {
    Map<ReservationInterval, Resource> req =
        new TreeMap<ReservationInterval, Resource>();
    long win = 86400000 / 4 + 1;
    int cont = (int) Math.ceil(0.5 * totCont);
    req.put(new ReservationInterval(initTime, initTime + win),
        ReservationSystemUtil.toResource(ReservationRequest.newInstance(Resource
            .newInstance(1024, 1), cont)));
    assertTrue(plan.toString(),
        plan.addReservation(new InMemoryReservationAllocation(
            ReservationSystemTestUtil.getNewReservationId(), null, "u1",

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/reservation/TestGreedyReservationAgent.java
hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/reservation/TestInMemoryPlan.java
    ReservationDefinition rDef =
        createSimpleReservationDefinition(start, start + alloc.length,
            alloc.length, allocations.values());
    Map<ReservationInterval, Resource> allocs =
        ReservationSystemUtil.toResources(allocations);
    ReservationAllocation rAllocation =
        new InMemoryReservationAllocation(reservationID, rDef, user, planName,
            start, start + alloc.length, allocs, resCalc, minAlloc);
    Assert.assertNull(plan.getReservationById(reservationID));
    try {
      plan.addReservation(rAllocation);
    ReservationDefinition rDef =
        createSimpleReservationDefinition(start, start + alloc.length,
            alloc.length, allocations.values());
    Map<ReservationInterval, Resource> allocs = ReservationSystemUtil.toResources
        (allocations);
    ReservationAllocation rAllocation =
        new InMemoryReservationAllocation(reservationID, rDef, user, planName,
            start, start + alloc.length, allocs, resCalc, minAlloc);
    Assert.assertNull(plan.getReservationById(reservationID));
    try {
      plan.addReservation(rAllocation);
    ReservationDefinition rDef =
        createSimpleReservationDefinition(start, start + alloc.length,
            alloc.length, allocations.values());
    Map<ReservationInterval, Resource> allocs = ReservationSystemUtil.toResources
        (allocations);
    ReservationAllocation rAllocation =
        new InMemoryReservationAllocation(reservationID, rDef, user, planName,
            start, start + alloc.length, allocs, resCalc, minAlloc);
    Assert.assertNull(plan.getReservationById(reservationID));
    try {
      plan.addReservation(rAllocation);
    ReservationDefinition rDef =
        createSimpleReservationDefinition(start, start + alloc.length,
            alloc.length, allocations.values());
    Map<ReservationInterval, Resource> allocs = ReservationSystemUtil.toResources
        (allocations);
    ReservationAllocation rAllocation =
        new InMemoryReservationAllocation(reservationID, rDef, user, planName,
            start, start + alloc.length, allocs, resCalc, minAlloc);
    Assert.assertNull(plan.getReservationById(reservationID));
    try {
      plan.addReservation(rAllocation);
    rDef =
        createSimpleReservationDefinition(start, start + updatedAlloc.length,
            updatedAlloc.length, allocations.values());
    Map<ReservationInterval, Resource> updatedAllocs =
        ReservationSystemUtil.toResources(allocations);
    rAllocation =
        new InMemoryReservationAllocation(reservationID, rDef, user, planName,
            start, start + updatedAlloc.length, updatedAllocs, resCalc,
            minAlloc);
    try {
      plan.updateReservation(rAllocation);
    } catch (PlanningException e) {
    ReservationDefinition rDef =
        createSimpleReservationDefinition(start, start + alloc.length,
            alloc.length, allocations.values());
    Map<ReservationInterval, Resource> allocs =
        ReservationSystemUtil.toResources(allocations);
    ReservationAllocation rAllocation =
        new InMemoryReservationAllocation(reservationID, rDef, user, planName,
            start, start + alloc.length, allocs, resCalc, minAlloc);
    Assert.assertNull(plan.getReservationById(reservationID));
    try {
      plan.updateReservation(rAllocation);
    ReservationDefinition rDef =
        createSimpleReservationDefinition(start, start + alloc.length,
            alloc.length, allocations.values());
    Map<ReservationInterval, Resource> allocs =
        ReservationSystemUtil.toResources(allocations);
    ReservationAllocation rAllocation =
        new InMemoryReservationAllocation(reservationID, rDef, user, planName,
            start, start + alloc.length, allocs, resCalc, minAlloc);
    Assert.assertNull(plan.getReservationById(reservationID));
    try {
      plan.addReservation(rAllocation);
    ReservationDefinition rDef1 =
        createSimpleReservationDefinition(start, start + alloc1.length,
            alloc1.length, allocations1.values());
    Map<ReservationInterval, Resource> allocs1 =
        ReservationSystemUtil.toResources(allocations1);
    ReservationAllocation rAllocation =
        new InMemoryReservationAllocation(reservationID1, rDef1, user,
            planName, start, start + alloc1.length, allocs1, resCalc,
            minAlloc);
    Assert.assertNull(plan.getReservationById(reservationID1));
    try {
    ReservationDefinition rDef2 =
        createSimpleReservationDefinition(start, start + alloc2.length,
            alloc2.length, allocations2.values());
    Map<ReservationInterval, Resource> allocs2 =
        ReservationSystemUtil.toResources(allocations2);
    rAllocation =
        new InMemoryReservationAllocation(reservationID2, rDef2, user,
            planName, start, start + alloc2.length, allocs2, resCalc,
            minAlloc);
    Assert.assertNull(plan.getReservationById(reservationID2));
    try {

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/reservation/TestInMemoryReservationAllocation.java
    ReservationDefinition rDef =
        createSimpleReservationDefinition(start, start + alloc.length + 1,
            alloc.length);
    Map<ReservationInterval, Resource> allocations =
        generateAllocation(start, alloc, false, false);
    ReservationAllocation rAllocation =
        new InMemoryReservationAllocation(reservationID, rDef, user, planName,
    ReservationDefinition rDef =
        createSimpleReservationDefinition(start, start + alloc.length + 1,
            alloc.length);
    Map<ReservationInterval, Resource> allocations =
        generateAllocation(start, alloc, true, false);
    ReservationAllocation rAllocation =
        new InMemoryReservationAllocation(reservationID, rDef, user, planName,
    ReservationDefinition rDef =
        createSimpleReservationDefinition(start, start + alloc.length + 1,
            alloc.length);
    Map<ReservationInterval, Resource> allocations =
        generateAllocation(start, alloc, true, false);
    ReservationAllocation rAllocation =
        new InMemoryReservationAllocation(reservationID, rDef, user, planName,
    ReservationDefinition rDef =
        createSimpleReservationDefinition(start, start + alloc.length + 1,
            alloc.length);
    Map<ReservationInterval, Resource> allocations =
        new HashMap<ReservationInterval, Resource>();
    ReservationAllocation rAllocation =
        new InMemoryReservationAllocation(reservationID, rDef, user, planName,
            start, start + alloc.length + 1, allocations, resCalc, minAlloc);
    ReservationDefinition rDef =
        createSimpleReservationDefinition(start, start + alloc.length + 1,
            alloc.length);
    boolean isGang = true;
    Map<ReservationInterval, Resource> allocations =
        generateAllocation(start, alloc, false, isGang);
    ReservationAllocation rAllocation =
        new InMemoryReservationAllocation(reservationID, rDef, user, planName,
            start, start + alloc.length + 1, allocations, resCalc, minAlloc,
            isGang);
    doAssertions(rAllocation, reservationID, rDef, allocations, start, alloc);
    Assert.assertTrue(rAllocation.containsGangs());
    for (int i = 0; i < alloc.length; i++) {

  private void doAssertions(ReservationAllocation rAllocation,
      ReservationId reservationID, ReservationDefinition rDef,
      Map<ReservationInterval, Resource> allocations, int start,
      int[] alloc) {
    Assert.assertEquals(reservationID, rAllocation.getReservationId());
    Assert.assertEquals(rDef, rAllocation.getReservationDefinition());
    return rDef;
  }

  private Map<ReservationInterval, Resource> generateAllocation(
      int startTime, int[] alloc, boolean isStep, boolean isGang) {
    Map<ReservationInterval, Resource> req =
        new HashMap<ReservationInterval, Resource>();
    int numContainers = 0;
    for (int i = 0; i < alloc.length; i++) {
      if (isStep) {
      if (isGang) {
        rr.setConcurrency(numContainers);
      }
      req.put(new ReservationInterval(startTime + i, startTime + i + 1),
          ReservationSystemUtil.toResource(rr));
    }
    return req;
  }

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/reservation/TestRLESparseResourceAllocation.java
        new RLESparseResourceAllocation(resCalc, minAlloc);
    int[] alloc = { 10, 10, 10, 10, 10, 10 };
    int start = 100;
    Set<Entry<ReservationInterval, Resource>> inputs =
        generateAllocation(start, alloc, false).entrySet();
    for (Entry<ReservationInterval, Resource> ip : inputs) {
      rleSparseVector.addInterval(ip.getKey(), ip.getValue());
    }
    LOG.info(rleSparseVector.toString());
    }
    Assert.assertEquals(Resource.newInstance(0, 0),
        rleSparseVector.getCapacityAtTime(start + alloc.length + 2));
    for (Entry<ReservationInterval, Resource> ip : inputs) {
      rleSparseVector.removeInterval(ip.getKey(), ip.getValue());
    }
    LOG.info(rleSparseVector.toString());
        new RLESparseResourceAllocation(resCalc, minAlloc);
    int[] alloc = { 10, 10, 10, 10, 10, 10 };
    int start = 100;
    Set<Entry<ReservationInterval, Resource>> inputs =
        generateAllocation(start, alloc, true).entrySet();
    for (Entry<ReservationInterval, Resource> ip : inputs) {
      rleSparseVector.addInterval(ip.getKey(), ip.getValue());
    }
    LOG.info(rleSparseVector.toString());
    }
    Assert.assertEquals(Resource.newInstance(0, 0),
        rleSparseVector.getCapacityAtTime(start + alloc.length + 2));
    for (Entry<ReservationInterval, Resource> ip : inputs) {
      rleSparseVector.removeInterval(ip.getKey(),ip.getValue());
    }
    LOG.info(rleSparseVector.toString());
        new RLESparseResourceAllocation(resCalc, minAlloc);
    int[] alloc = { 0, 5, 10, 10, 5, 0 };
    int start = 100;
    Set<Entry<ReservationInterval, Resource>> inputs =
        generateAllocation(start, alloc, true).entrySet();
    for (Entry<ReservationInterval, Resource> ip : inputs) {
      rleSparseVector.addInterval(ip.getKey(), ip.getValue());
    }
    LOG.info(rleSparseVector.toString());
    }
    Assert.assertEquals(Resource.newInstance(0, 0),
        rleSparseVector.getCapacityAtTime(start + alloc.length + 2));
    for (Entry<ReservationInterval, Resource> ip : inputs) {
      rleSparseVector.removeInterval(ip.getKey(), ip.getValue());
    }
    LOG.info(rleSparseVector.toString());
    RLESparseResourceAllocation rleSparseVector =
        new RLESparseResourceAllocation(resCalc, minAlloc);
    rleSparseVector.addInterval(new ReservationInterval(0, Long.MAX_VALUE),
        Resource.newInstance(0, 0));
    LOG.info(rleSparseVector.toString());
    Assert.assertEquals(Resource.newInstance(0, 0),
        rleSparseVector.getCapacityAtTime(new Random().nextLong()));
    Assert.assertTrue(rleSparseVector.isEmpty());
  }

  private Map<ReservationInterval, Resource> generateAllocation(
      int startTime, int[] alloc, boolean isStep) {
    Map<ReservationInterval, Resource> req =
        new HashMap<ReservationInterval, Resource>();
    int numContainers = 0;
    for (int i = 0; i < alloc.length; i++) {
      if (isStep) {
        numContainers = alloc[i];
      }
      req.put(new ReservationInterval(startTime + i, startTime + i + 1),
          ReservationSystemUtil.toResource(ReservationRequest.newInstance(
              Resource.newInstance(1024, 1), (numContainers))));
    }
    return req;
  }

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/reservation/TestSimpleCapacityReplanner.java
