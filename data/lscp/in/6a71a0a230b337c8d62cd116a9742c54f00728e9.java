hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/ReservationQueue.java

  private PlanQueue parent;

  public ReservationQueue(CapacitySchedulerContext cs, String queueName,
      PlanQueue parent) throws IOException {
    super(cs, queueName, parent, null);
    updateQuotas(parent.getUserLimitForReservation(),
        parent.getUserLimitFactor(),
    }
    setCapacity(capacity);
    setAbsoluteCapacity(getParent().getAbsoluteCapacity() * getCapacity());
    setMaxCapacity(entitlement.getMaxCapacity());

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/capacity/TestReservationQueue.java

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

  CapacitySchedulerConfiguration csConf;
  CapacitySchedulerContext csContext;
  final static int DEF_MAX_APPS = 10000;
  final static int GB = 1024;
  private final ResourceCalculator resourceCalculator =
      new DefaultResourceCalculator();
    PlanQueue pq = new PlanQueue(csContext, "root", null, null);
    reservationQueue = new ReservationQueue(csContext, "a", pq);
  }

  private void validateReservationQueue(double capacity) {
    assertTrue(" actual capacity: " + reservationQueue.getCapacity(),
        reservationQueue.getCapacity() - capacity < CSQueueUtils.EPSILON);
    assertEquals(reservationQueue.maxApplications, DEF_MAX_APPS);
    assertEquals(reservationQueue.maxApplicationsPerUser, DEF_MAX_APPS);
  }

  @Test

    reservationQueue.setCapacity(1.0F);
    validateReservationQueue(1);
    reservationQueue.setEntitlement(new QueueEntitlement(0.9f, 1f));
    validateReservationQueue(0.9);
    reservationQueue.setEntitlement(new QueueEntitlement(1f, 1f));
    validateReservationQueue(1);
    reservationQueue.setEntitlement(new QueueEntitlement(0f, 1f));
    validateReservationQueue(0);

    try {
      reservationQueue.setEntitlement(new QueueEntitlement(1.1f, 1f));
      fail();
    } catch (SchedulerDynamicEditException iae) {
      validateReservationQueue(1);
    }

    try {
      fail();
    } catch (SchedulerDynamicEditException iae) {
      validateReservationQueue(1);
    }

  }

