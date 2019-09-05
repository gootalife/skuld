hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/security/LdapGroupsMapping.java
  public static final String GROUP_NAME_ATTR_KEY = LDAP_CONFIG_PREFIX + ".search.attr.group.name";
  public static final String GROUP_NAME_ATTR_DEFAULT = "cn";

  public static final String POSIX_UID_ATTR_KEY = LDAP_CONFIG_PREFIX + ".posix.attr.uid.name";
  public static final String POSIX_UID_ATTR_DEFAULT = "uidNumber";

  public static final String POSIX_GID_ATTR_KEY = LDAP_CONFIG_PREFIX + ".posix.attr.gid.name";
  public static final String POSIX_GID_ATTR_DEFAULT = "gidNumber";

  public static final String POSIX_GROUP = "posixGroup";
  public static final String POSIX_ACCOUNT = "posixAccount";

  private String userSearchFilter;
  private String groupMemberAttr;
  private String groupNameAttr;
  private String posixUidAttr;
  private String posixGidAttr;
  private boolean isPosix;

  public static int RECONNECT_RETRY_COUNT = 3;
      if (isPosix) {
        String gidNumber = null;
        String uidNumber = null;
        Attribute gidAttribute = result.getAttributes().get(posixGidAttr);
        Attribute uidAttribute = result.getAttributes().get(posixUidAttr);
        if (gidAttribute != null) {
          gidNumber = gidAttribute.get().toString();
        }
        if (uidNumber != null && gidNumber != null) {
          groupResults =
              ctx.search(baseDN,
                  "(&"+ groupSearchFilter + "(|(" + posixGidAttr + "={0})" +
                      "(" + groupMemberAttr + "={1})))",
                  new Object[] { gidNumber, uidNumber },
                  SEARCH_CONTROLS);
        conf.get(GROUP_MEMBERSHIP_ATTR_KEY, GROUP_MEMBERSHIP_ATTR_DEFAULT);
    groupNameAttr =
        conf.get(GROUP_NAME_ATTR_KEY, GROUP_NAME_ATTR_DEFAULT);
    posixUidAttr =
        conf.get(POSIX_UID_ATTR_KEY, POSIX_UID_ATTR_DEFAULT);
    posixGidAttr =
        conf.get(POSIX_GID_ATTR_KEY, POSIX_GID_ATTR_DEFAULT);

    int dirSearchTimeout = conf.getInt(DIRECTORY_SEARCH_TIMEOUT, DIRECTORY_SEARCH_TIMEOUT_DEFAULT);
    SEARCH_CONTROLS.setTimeLimit(dirSearchTimeout);
    SEARCH_CONTROLS.setReturningAttributes(
        new String[] {groupNameAttr, posixUidAttr, posixGidAttr});

    this.conf = conf;
  }

hadoop-common-project/hadoop-common/src/test/java/org/apache/hadoop/security/TestLdapGroupsMappingWithPosixGroup.java
    SearchResult mockUserResult = mock(SearchResult.class);
    when(mockUserNamingEnum.nextElement()).thenReturn(mockUserResult);

    Attribute mockUidNumberAttr = mock(Attribute.class);
    Attribute mockGidNumberAttr = mock(Attribute.class);
    Attribute mockUidAttr = mock(Attribute.class);
    Attributes mockAttrs = mock(Attributes.class);

    when(mockUidAttr.get()).thenReturn("some_user");
    when(mockUidNumberAttr.get()).thenReturn("700");
    when(mockGidNumberAttr.get()).thenReturn("600");
    when(mockAttrs.get(eq("uid"))).thenReturn(mockUidAttr);
    when(mockAttrs.get(eq("uidNumber"))).thenReturn(mockUidNumberAttr);
    when(mockAttrs.get(eq("gidNumber"))).thenReturn(mockGidNumberAttr);

    when(mockUserResult.getAttributes()).thenReturn(mockAttrs);
  }
    conf.set(LdapGroupsMapping.USER_SEARCH_FILTER_KEY,
        "(objectClass=posixAccount)");
    conf.set(LdapGroupsMapping.GROUP_MEMBERSHIP_ATTR_KEY, "memberUid");
    conf.set(LdapGroupsMapping.POSIX_UID_ATTR_KEY, "uidNumber");
    conf.set(LdapGroupsMapping.POSIX_GID_ATTR_KEY, "gidNumber");
    conf.set(LdapGroupsMapping.GROUP_NAME_ATTR_KEY, "cn");

    mappingSpy.setConf(conf);

    Assert.assertEquals(expectedGroups, groups);

    mappingSpy.getConf().set(LdapGroupsMapping.POSIX_UID_ATTR_KEY, "uid");

    Assert.assertEquals(expectedGroups, groups);

    verify(mockContext, times(searchTimes)).search(anyString(),
        anyString(),

