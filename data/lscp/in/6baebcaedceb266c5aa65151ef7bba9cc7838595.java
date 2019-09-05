hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/monitor/capacity/ProportionalCapacityPreemptionPolicy.java
                  SchedulerEventType.KILL_CONTAINER));
          preempted.remove(container);
        } else {
          if (preempted.get(container) != null) {
            continue;
          }
          rmContext.getDispatcher().getEventHandler().handle(
              new ContainerPreemptEvent(appAttemptId, container,
                  SchedulerEventType.PREEMPT_CONTAINER));
          preempted.put(container, clock.getTime());
        }
      }
    }

    for (Iterator<RMContainer> i = preempted.keySet().iterator(); i.hasNext();){

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/test/java/org/apache/hadoop/yarn/server/resourcemanager/monitor/capacity/TestProportionalCapacityPreemptionPolicy.java
    when(mClock.getTime()).thenReturn(killTime / 2);
    policy.editSchedule();
    verify(mDisp, times(10)).handle(argThat(new IsPreemptionRequestFor(appC)));

    when(mClock.getTime()).thenReturn(killTime + 1);
    policy.editSchedule();
    verify(mDisp, times(20)).handle(evtCaptor.capture());
    List<ContainerPreemptEvent> events = evtCaptor.getAllValues();
    for (ContainerPreemptEvent e : events.subList(20, 20)) {
      assertEquals(appC, e.getAppId());
      assertEquals(KILL_CONTAINER, e.getType());
    }

