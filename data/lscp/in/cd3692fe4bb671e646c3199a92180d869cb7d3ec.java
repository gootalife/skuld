hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/AclTransformation.java
  private static List<AclEntry> buildAndValidateAcl(
      ArrayList<AclEntry> aclBuilder) throws AclException {
    aclBuilder.trimToSize();
    Collections.sort(aclBuilder, ACL_ENTRY_COMPARATOR);
      }
      prevEntry = entry;
    }

    ScopedAclEntries scopedEntries = new ScopedAclEntries(aclBuilder);
    checkMaxEntries(scopedEntries);

    for (AclEntryType type: EnumSet.of(USER, GROUP, OTHER)) {
      AclEntry accessEntryKey = new AclEntry.Builder().setScope(ACCESS)
        .setType(type).build();
    return Collections.unmodifiableList(aclBuilder);
  }

  private static void checkMaxEntries(ScopedAclEntries scopedEntries)
      throws AclException {
    List<AclEntry> accessEntries = scopedEntries.getAccessEntries();
    List<AclEntry> defaultEntries = scopedEntries.getDefaultEntries();
    if (accessEntries.size() > MAX_ENTRIES) {
      throw new AclException("Invalid ACL: ACL has " + accessEntries.size()
          + " access entries, which exceeds maximum of " + MAX_ENTRIES + ".");
    }
    if (defaultEntries.size() > MAX_ENTRIES) {
      throw new AclException("Invalid ACL: ACL has " + defaultEntries.size()
          + " default entries, which exceeds maximum of " + MAX_ENTRIES + ".");
    }
  }

    public ValidatedAclSpec(List<AclEntry> aclSpec) throws AclException {
      Collections.sort(aclSpec, ACL_ENTRY_COMPARATOR);
      checkMaxEntries(new ScopedAclEntries(aclSpec));
      this.aclSpec = aclSpec;
    }


hadoop-hdfs-project/hadoop-hdfs/src/test/java/org/apache/hadoop/hdfs/server/namenode/TestAclTransformation.java
import org.junit.Test;

import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hdfs.protocol.AclException;

public class TestAclTransformation {

  private static final List<AclEntry> ACL_SPEC_TOO_LARGE;
  private static final List<AclEntry> ACL_SPEC_DEFAULT_TOO_LARGE;
  static {
    ACL_SPEC_TOO_LARGE = Lists.newArrayListWithCapacity(33);
    ACL_SPEC_DEFAULT_TOO_LARGE = Lists.newArrayListWithCapacity(33);
    for (int i = 0; i < 33; ++i) {
      ACL_SPEC_TOO_LARGE.add(aclEntry(ACCESS, USER, "user" + i, ALL));
      ACL_SPEC_DEFAULT_TOO_LARGE.add(aclEntry(DEFAULT, USER, "user" + i, ALL));
    }
  }

    filterAclEntriesByAclSpec(existing, ACL_SPEC_TOO_LARGE);
  }

  @Test(expected = AclException.class)
  public void testFilterDefaultAclEntriesByAclSpecInputTooLarge()
      throws AclException {
    List<AclEntry> existing = new ImmutableList.Builder<AclEntry>()
        .add(aclEntry(DEFAULT, USER, ALL))
        .add(aclEntry(DEFAULT, GROUP, READ))
        .add(aclEntry(DEFAULT, OTHER, NONE))
        .build();
    filterAclEntriesByAclSpec(existing, ACL_SPEC_DEFAULT_TOO_LARGE);
  }

  @Test
  public void testFilterDefaultAclEntries() throws AclException {
    List<AclEntry> existing = new ImmutableList.Builder<AclEntry>()
    mergeAclEntries(existing, ACL_SPEC_TOO_LARGE);
  }

  @Test(expected=AclException.class)
  public void testMergeAclDefaultEntriesInputTooLarge() throws AclException {
    List<AclEntry> existing = new ImmutableList.Builder<AclEntry>()
      .add(aclEntry(DEFAULT, USER, ALL))
      .add(aclEntry(DEFAULT, GROUP, READ))
      .add(aclEntry(DEFAULT, OTHER, NONE))
      .build();
    mergeAclEntries(existing, ACL_SPEC_DEFAULT_TOO_LARGE);
  }

  @Test(expected=AclException.class)
  public void testMergeAclEntriesResultTooLarge() throws AclException {
    ImmutableList.Builder<AclEntry> aclBuilder =
    mergeAclEntries(existing, aclSpec);
  }

  @Test(expected = AclException.class)
  public void testMergeAclDefaultEntriesResultTooLarge() throws AclException {
    ImmutableList.Builder<AclEntry> aclBuilder =
        new ImmutableList.Builder<AclEntry>()
        .add(aclEntry(DEFAULT, USER, ALL));
    for (int i = 1; i <= 28; ++i) {
      aclBuilder.add(aclEntry(DEFAULT, USER, "user" + i, READ));
    }
    aclBuilder
    .add(aclEntry(DEFAULT, GROUP, READ))
    .add(aclEntry(DEFAULT, MASK, READ))
    .add(aclEntry(DEFAULT, OTHER, NONE));
    List<AclEntry> existing = aclBuilder.build();
    List<AclEntry> aclSpec = Lists.newArrayList(
         aclEntry(DEFAULT, USER, "bruce", READ));
    mergeAclEntries(existing, aclSpec);
  }

  @Test(expected=AclException.class)
  public void testMergeAclEntriesDuplicateEntries() throws AclException {
    List<AclEntry> existing = new ImmutableList.Builder<AclEntry>()
    replaceAclEntries(existing, ACL_SPEC_TOO_LARGE);
  }

  @Test(expected=AclException.class)
  public void testReplaceAclDefaultEntriesInputTooLarge() throws AclException {
    List<AclEntry> existing = new ImmutableList.Builder<AclEntry>()
      .add(aclEntry(DEFAULT, USER, ALL))
      .add(aclEntry(DEFAULT, GROUP, READ))
      .add(aclEntry(DEFAULT, OTHER, NONE))
      .build();
    replaceAclEntries(existing, ACL_SPEC_DEFAULT_TOO_LARGE);
  }

  @Test(expected=AclException.class)
  public void testReplaceAclEntriesResultTooLarge() throws AclException {
    List<AclEntry> existing = new ImmutableList.Builder<AclEntry>()

