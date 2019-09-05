hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/fs/HarFs.java
public class HarFs extends DelegateToFileSystem {
  HarFs(final URI theUri, final Configuration conf)
      throws IOException, URISyntaxException {
    super(theUri, new HarFileSystem(), conf, "har", false);
  }

  @Override

hadoop-common-project/hadoop-common/src/test/java/org/apache/hadoop/fs/TestHarFileSystemBasics.java
    }
  }

  @Test
  public void testHarFsWithoutAuthority() throws Exception {
    final URI uri = harFileSystem.getUri();
    Assert.assertNull("har uri authority not null: " + uri, uri.getAuthority());
    FileContext.getFileContext(uri, conf);
  }

}

