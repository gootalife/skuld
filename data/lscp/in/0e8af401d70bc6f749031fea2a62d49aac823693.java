hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-applicationhistoryservice/src/main/java/org/apache/hadoop/yarn/server/timeline/LeveldbTimelineStore.java
      iterator = new LeveldbIterator(db);
      iterator.seek(prefix);

      if (fields == null) {
        fields = EnumSet.allOf(Field.class);
      }
      return getEntity(entityId, entityType, revStartTime, fields, iterator,
          prefix, prefix.length);
    } catch(DBException e) {
  private static TimelineEntity getEntity(String entityId, String entityType,
      Long startTime, EnumSet<Field> fields, LeveldbIterator iterator,
      byte[] prefix, int prefixlen) throws IOException {
    TimelineEntity entity = new TimelineEntity();
    boolean events = false;
    boolean lastEvent = false;
      String entityType, Long limit, Long starttime, Long endtime,
      String fromId, Long fromTs, Collection<NameValuePair> secondaryFilters,
      EnumSet<Field> fields, CheckAcl checkAcl) throws IOException {
    if (fields == null) {
      fields = EnumSet.allOf(Field.class);
    }
    boolean addPrimaryFilters = false;
    boolean addOtherInfo = false;
    if (secondaryFilters != null && secondaryFilters.size() > 0) {
      if (!fields.contains(Field.PRIMARY_FILTERS)) {
        fields.add(Field.PRIMARY_FILTERS);
        addPrimaryFilters = true;
      }
      if (!fields.contains(Field.OTHER_INFO)) {
        fields.add(Field.OTHER_INFO);
        addOtherInfo = true;
      }
    }

    LeveldbIterator iterator = null;
    try {
      KeyBuilder kb = KeyBuilder.newInstance().add(base).add(entityType);
            entity.setDomainId(DEFAULT_DOMAIN_ID);
          }
          if (checkAcl == null || checkAcl.check(entity)) {
            if (addPrimaryFilters) {
              entity.setPrimaryFilters(null);
            }
            if (addOtherInfo) {
              entity.setOtherInfo(null);
            }
            entities.addEntity(entity);
          }
        }

hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-applicationhistoryservice/src/test/java/org/apache/hadoop/yarn/server/timeline/TimelineStoreTestUtils.java
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
        primaryFilter, secondaryFilters, null, null).getEntities();
  }

  protected List<TimelineEntity> getEntitiesWithFilters(String entityType,
      NameValuePair primaryFilter, Collection<NameValuePair> secondaryFilters,
      EnumSet<Field> fields) throws IOException {
    return store.getEntities(entityType, null, null, null, null, null,
        primaryFilter, secondaryFilters, fields, null).getEntities();
  }

  protected List<TimelineEntity> getEntities(String entityType, Long limit,
      Long windowStart, Long windowEnd, NameValuePair primaryFilter,
      EnumSet<Field> fields) throws IOException {
  }

  public void testGetEntitiesWithSecondaryFilters() throws IOException {
    for (int i = 0; i < 4; ++i) {
      EnumSet<Field> fields = null;
      if (i == 1) {
        fields = EnumSet.noneOf(Field.class);
      } else if (i == 2) {
        fields = EnumSet.of(Field.PRIMARY_FILTERS);
      } else if (i == 3) {
        fields = EnumSet.of(Field.OTHER_INFO);
      }
      List<TimelineEntity> entities = getEntitiesWithFilters("type_1", null,
          goodTestingFilters, fields);
      assertEquals(3, entities.size());
      verifyEntityInfo(entityId1, entityType1,
          (i == 0 ? events1 : null),
          (i == 0 ? EMPTY_REL_ENTITIES : null),
          (i == 0 || i == 2 ? primaryFilters : null),
          (i == 0 || i == 3 ? otherInfo : null), entities.get(0), domainId1);
      verifyEntityInfo(entityId1b, entityType1,
          (i == 0 ? events1 : null),
          (i == 0 ? EMPTY_REL_ENTITIES : null),
          (i == 0 || i == 2 ? primaryFilters : null),
          (i == 0 || i == 3 ? otherInfo : null), entities.get(1), domainId1);
      verifyEntityInfo(entityId6, entityType1,
          (i == 0 ? EMPTY_EVENTS : null),
          (i == 0 ? EMPTY_REL_ENTITIES : null),
          (i == 0 || i == 2 ? primaryFilters : null),
          (i == 0 || i == 3 ? otherInfo : null), entities.get(2), domainId2);

      entities =
          getEntitiesWithFilters("type_1", userFilter, goodTestingFilters, fields);
      assertEquals(3, entities.size());
      if (i == 0) {
        verifyEntityInfo(entityId1, entityType1,
            (i == 0 ? events1 : null),
            (i == 0 ? EMPTY_REL_ENTITIES : null),
            (i == 0 || i == 2 ? primaryFilters : null),
            (i == 0 || i == 3 ? otherInfo : null), entities.get(0), domainId1);
        verifyEntityInfo(entityId1b, entityType1,
            (i == 0 ? events1 : null),
            (i == 0 ? EMPTY_REL_ENTITIES : null),
            (i == 0 || i == 2 ? primaryFilters : null),
            (i == 0 || i == 3 ? otherInfo : null), entities.get(1), domainId1);
        verifyEntityInfo(entityId6, entityType1,
            (i == 0 ? EMPTY_EVENTS : null),
            (i == 0 ? EMPTY_REL_ENTITIES : null),
            (i == 0 || i == 2 ? primaryFilters : null),
            (i == 0 || i == 3 ? otherInfo : null), entities.get(2), domainId2);
      }

      entities = getEntitiesWithFilters("type_1", null,
          Collections.singleton(new NameValuePair("user", "none")), fields);
      assertEquals(0, entities.size());

      entities =
          getEntitiesWithFilters("type_1", null, badTestingFilters, fields);
      assertEquals(0, entities.size());

      entities =
          getEntitiesWithFilters("type_1", userFilter, badTestingFilters, fields);
      assertEquals(0, entities.size());

      entities =
          getEntitiesWithFilters("type_5", null, badTestingFilters, fields);
      assertEquals(0, entities.size());
    }
  }

  public void testGetEvents() throws IOException {

